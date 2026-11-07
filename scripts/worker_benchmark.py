"""Compare managed worker dispatch using the same workload and wire-level accounting."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import zipfile


ROOT = Path(__file__).resolve().parent.parent
BASELINE = "1f9a04c0b53d2377c369a2c9dbce07e3708c9b51"
COMMAND = Path("cmd/worker_benchmark")
EXECUTABLE = Path("_build/native/debug/build") / COMMAND / "worker_benchmark.exe"


def build(root):
    subprocess.run(["moon", "build", str(COMMAND), "--target", "native"],
                   cwd=root, check=True, timeout=120)
    return root / EXECUTABLE


def build_baseline(directory, ref):
    checkout = directory / "baseline"
    checkout.mkdir()
    archive = directory / "baseline.zip"
    subprocess.run(["git", "archive", "--format=zip", "-o", str(archive), ref],
                   cwd=ROOT, check=True, timeout=30)
    with zipfile.ZipFile(archive) as source:
        source.extractall(checkout)
    shutil.copytree(ROOT / COMMAND, checkout / COMMAND, dirs_exist_ok=True)
    return build(checkout)


async def exercise(executable, mode, upstream_port):
    delivered = {}
    confirmations = {}
    live_bytes = peak_bytes = peak_messages = total_deliveries = 0
    sessions = set()
    errors = []

    async def relay(reader, writer, downstream):
        nonlocal live_bytes, peak_bytes, peak_messages, total_deliveries
        while line := await reader.readline():
            fields = line.split()
            payload = b""
            if fields and fields[0] in (b"PUB", b"HPUB", b"MSG", b"HMSG"):
                command = fields[0]
                topic = fields[1]
                size = int(fields[-1])
                payload = await reader.readexactly(size + 2)
                if downstream:
                    if topic.startswith(b"moon.workerbench."):
                        reply = fields[3]
                        assert reply.startswith(b"$JS.ACK.") and reply not in delivered
                        cost = len(topic) + len(reply) + size
                        delivered[reply] = cost
                        live_bytes += cost
                        total_deliveries += 1
                        peak_messages = max(peak_messages, len(delivered))
                        peak_bytes = max(peak_bytes, live_bytes)
                    elif topic in confirmations:
                        assert size == 0, (topic, payload)
                        live_bytes -= delivered.pop(confirmations.pop(topic))
                elif command == b"PUB" and topic in delivered and len(fields) == 4:
                    assert payload == b"+ACK\r\n"
                    confirmations[fields[2]] = topic
            writer.write(line + payload)
            await writer.drain()

    async def peer(reader, writer):
        task = asyncio.current_task()
        sessions.add(task)
        upstream = None
        directions = []
        try:
            upstream_reader, upstream = await asyncio.open_connection("127.0.0.1", upstream_port)
            directions = [
                asyncio.create_task(relay(reader, upstream, False)),
                asyncio.create_task(relay(upstream_reader, writer, True)),
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
            sessions.discard(task)

    listener = await asyncio.start_server(peer, "127.0.0.1", 0)
    async with listener:
        port = listener.sockets[0].getsockname()[1]
        env = {**os.environ, "MOONNATS_WORKER_BENCH_URL": f"nats://127.0.0.1:{port}",
               "MOONNATS_WORKER_BENCH_MODE": mode}
        process = await asyncio.create_subprocess_exec(
            str(executable), env=env, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            async with asyncio.timeout(40):
                stdout, stderr = await process.communicate()
            assert process.returncode == 0, (process.returncode, stderr.decode(errors="replace"))
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
            for session in tuple(sessions):
                session.cancel()
            await asyncio.gather(*sessions, return_exceptions=True)
    assert not errors, errors
    result = json.loads(stdout)
    assert not delivered and not confirmations and live_bytes == 0
    assert total_deliveries == result["messages"]
    assert 0 < peak_messages <= 16 and 0 < peak_bytes <= 16384
    return {**result, "peak_inflight_messages": peak_messages, "peak_inflight_bytes": peak_bytes,
            "redeliveries": total_deliveries - result["messages"]}


async def run_benchmarks(executables, repetitions, port):
    for repetition in range(repetitions):
        order = executables if repetition % 2 == 0 else executables[::-1]
        for mode in ("sparse", "burst", "mixed"):
            for version, executable in order:
                report = await exercise(executable, mode, port)
                print(json.dumps({"version": version, "repetition": repetition + 1, **report}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", default=BASELINE)
    parser.add_argument("--current-only", action="store_true")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--upstream-port", type=int, default=14222)
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("--repetitions must be positive")
    current = build(ROOT)
    data = ROOT / ".test-data"
    data.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="worker-benchmark-", dir=data) as path:
        executables = [("current", current)]
        if not args.current_only:
            baseline = build_baseline(Path(path), args.baseline)
            executables.insert(0, (args.baseline, baseline))
        asyncio.run(run_benchmarks(executables, args.repetitions, args.upstream_port))


if __name__ == "__main__":
    main()
