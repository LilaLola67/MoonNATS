"""Inject client-side reconnect and flush faults at explicit test markers."""
import asyncio


async def run_client(executable, flag, marker, on_marker, env=None):
    process = await asyncio.create_subprocess_exec(
        str(executable), flag, env=env, stderr=asyncio.subprocess.PIPE,
    )
    seen = False
    try:
        async with asyncio.timeout(15):
            async for line in process.stderr:
                if line.strip() == marker:
                    assert not seen
                    seen = True
                    await on_marker()
                else:
                    print(line.decode(errors="replace"), end="", flush=True)
            code = await process.wait()
            assert code == 0 and seen, (flag, code, seen)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


async def check_buffer(executable, primary):
    async def drop_primary():
        primary.terminate()
        await asyncio.to_thread(primary.wait, 5)

    await run_client(executable, "--buffer", b"DROP_PRIMARY", drop_primary)


async def check_flush(executable, environ):
    connections = []
    tasks = set()
    errors = []
    held_ping = asyncio.Event()
    release_old_pong = asyncio.Event()
    new_pong_sent = False

    async def peer(reader, writer):
        nonlocal new_pong_sent
        task = asyncio.current_task()
        tasks.add(task)
        connections.append(writer)
        first = len(connections) == 1
        try:
            writer.write(b'INFO {"server_id":"fault","max_payload":1024,"headers":true}\r\n')
            await writer.drain()
            assert (await reader.readline()).startswith(b"CONNECT ")
            assert await reader.readline() == b"PING\r\n"
            writer.write(b"PONG\r\n")
            await writer.drain()
            assert await reader.readline() == b"PING\r\n"
            if first:
                held_ping.set()
                await release_old_pong.wait()
            else:
                await asyncio.sleep(.15)
            writer.write(b"PONG\r\n")
            await writer.drain()
            if not first:
                new_pong_sent = True
            assert await reader.readline() == b"", "Unexpected PING on the retired connection"
        except Exception as error:
            errors.append(error)
        finally:
            writer.close()
            tasks.discard(task)

    async def release_late_pong():
        assert held_ping.is_set()
        release_old_pong.set()

    listener = await asyncio.start_server(peer, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    async with listener:
        try:
            await run_client(
                executable, "--flush", b"FLUSH_TIMEOUT", release_late_pong,
                env={**environ, "MOONNATS_FAULT_URL": f"nats://127.0.0.1:{port}"},
            )
        finally:
            for writer in connections:
                writer.close()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    assert not errors, errors
    assert len(connections) == 2 and new_pong_sent
