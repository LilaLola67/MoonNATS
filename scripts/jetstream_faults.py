"""Drop complete JetStream replies after a real server has processed the request."""
import asyncio
import json
import os
import sys

import nats
from nats.js.errors import NotFoundError


STREAM = "MN_ACK_FAULTS"
UPSTREAM_PORT = 14222


async def main(executable):
    dropped = {"first": 0, "unknown": 0, "ack": 0}
    tasks = set()
    errors = []

    async def forward_replies(reader, writer):
        while line := await reader.readline():
            fields = line.split()
            payload = b""
            if fields[0] in (b"MSG", b"HMSG"):
                payload = await reader.readexactly(int(fields[-1]) + 2)
                if fields[0] == b"MSG" and fields[1].startswith(b"_INBOX."):
                    if payload == b"\r\n" and not dropped["ack"]:
                        assert dropped["first"] == 1
                        dropped["ack"] += 1
                        continue
                    if payload.startswith(b"{"):
                        reply = json.loads(payload)
                        if reply.get("stream") == STREAM:
                            seq = reply["seq"]
                            if seq == 1 and not dropped["first"]:
                                assert not reply.get("duplicate")
                                dropped["first"] += 1
                                continue
                            if seq == 3:
                                dropped["unknown"] += 1
                                continue
            writer.write(line + payload)
            await writer.drain()

    async def forward_requests(reader, writer):
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()

    async def proxy(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        upstream = None
        directions = []
        try:
            server_reader, upstream = await asyncio.open_connection("127.0.0.1", UPSTREAM_PORT)
            directions = [
                asyncio.create_task(forward_requests(reader, upstream)),
                asyncio.create_task(forward_replies(server_reader, writer)),
            ]
            done, _ = await asyncio.wait(directions, return_when=asyncio.FIRST_COMPLETED)
            for finished in done:
                finished.result()
        except Exception as error:
            errors.append(error)
        finally:
            for direction in directions:
                direction.cancel()
            await asyncio.gather(*directions, return_exceptions=True)
            writer.close()
            if upstream is not None:
                upstream.close()
            tasks.discard(task)

    oracle = await nats.connect(f"nats://127.0.0.1:{UPSTREAM_PORT}")
    js = oracle.jetstream()
    try:
        try:
            await js.delete_stream(STREAM)
        except NotFoundError:
            pass
        listener = await asyncio.start_server(proxy, "127.0.0.1", 0)
        async with listener:
            port = listener.sockets[0].getsockname()[1]
            process = await asyncio.create_subprocess_exec(
                executable, "--jetstream",
                env={**os.environ, "MOONNATS_FAULT_URL": f"nats://127.0.0.1:{port}"},
            )
            try:
                async with asyncio.timeout(20):
                    code = await process.wait()
                    assert code == 0, f"MoonBit fault client exited with {code}"
            finally:
                if process.returncode is None:
                    process.kill()
                    await process.wait()
        assert not errors, errors
        assert dropped["first"] == dropped["ack"] == 1 and dropped["unknown"] >= 2, dropped
        stream = await js.stream_info(STREAM)
        worker = await js.consumer_info(STREAM, "worker")
        assert stream.state.messages == stream.state.last_seq == 3
        assert worker.ack_floor.stream_seq == 2 and worker.num_ack_pending == 0
        assert worker.num_pending == 1
        drain = await js.pull_subscribe_bind(consumer="drain", stream=STREAM)
        [redelivery] = await drain.fetch(batch=1, timeout=3)
        assert redelivery.data == b"unknown" and redelivery.metadata.num_delivered == 2
        await redelivery.ack_sync(timeout=2)
        print("JetStream oracle: lost replies, confirmed ACK floor, and redelivery after drain timeout PASS")
    finally:
        for task in tuple(tasks):
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await js.delete_stream(STREAM)
        except NotFoundError:
            pass
        finally:
            await oracle.close()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
