"""Run load and failover acceptance against three real, replicated NATS nodes."""
import argparse
import asyncio
from contextlib import ExitStack
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import urllib.request

import nats


ROOT = Path(__file__).resolve().parent.parent
STREAM = "MOON_CLUSTER_JOBS"
DURABLE = "workers"
CLIENT = ROOT / "_build/native/debug/build/cmd/cluster_integration/cluster_integration.exe"
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def allocate_ports():
    with ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(9)]
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
        return [listener.getsockname()[1] for listener in sockets]


class Node:
    def __init__(self, name, ports, directory, server):
        self.name = name
        self.client_port, self.route_port, self.monitor_port = ports
        self.url = f"nats://127.0.0.1:{self.client_port}"
        self.config = directory / f"{name}.conf"
        self.log = directory / f"{name}.log"
        self.store = directory / name
        self.server = server
        self.process = None

    def configure(self, peers):
        routes = [f"nats-route://127.0.0.1:{peer.route_port}" for peer in peers if peer is not self]
        self.config.write_text(
            f'server_name: "{self.name}"\n'
            f'listen: "127.0.0.1:{self.client_port}"\n'
            f'http: "127.0.0.1:{self.monitor_port}"\n'
            f'jetstream {{ store_dir: {json.dumps(self.store.as_posix(), ensure_ascii=False)} }}\n'
            f'cluster {{ name: "MoonNATS_TEST", listen: "127.0.0.1:{self.route_port}", '
            f'routes: {json.dumps(routes)} }}\n', encoding="utf-8")

    def start(self):
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        with self.log.open("ab") as output:
            self.process = subprocess.Popen([str(self.server), "-c", str(self.config)],
                                            cwd=ROOT, stdout=output, stderr=output,
                                            creationflags=flags)

    async def stop(self, *, crash=False):
        if self.process is None or self.process.poll() is not None:
            return
        if crash:
            self.process.kill()
        elif os.name == "nt":
            try:
                self.process.send_signal(signal.CTRL_BREAK_EVENT)
            except OSError:
                self.process.terminate()
        else:
            self.process.terminate()
        try:
            await asyncio.to_thread(self.process.wait, 8)
        except subprocess.TimeoutExpired:
            self.process.kill()
            await asyncio.to_thread(self.process.wait, 5)
            raise

    def monitor(self, endpoint):
        with HTTP.open(f"http://127.0.0.1:{self.monitor_port}/{endpoint}", timeout=1) as reply:
            return json.load(reply)


async def wait_routes(nodes):
    async with asyncio.timeout(25):
        while True:
            try:
                names = {node.name for node in nodes}
                for node in nodes:
                    if node.process.poll() is not None:
                        raise RuntimeError(f"{node.name} exited before routes became ready")
                routes = await asyncio.gather(*(asyncio.to_thread(node.monitor, "routez") for node in nodes))
                if all(names - {node.name} <= {route["remote_name"] for route in report["routes"]}
                       for node, report in zip(nodes, routes)):
                    status = await asyncio.to_thread(nodes[0].monitor, "jsz")
                    if status.get("meta_cluster", {}).get("leader") in names:
                        return
            except (OSError, KeyError):
                pass
            await asyncio.sleep(0.1)


async def connect_oracle(node):
    async def ignore_error(_):
        pass

    return await nats.connect(node.url, allow_reconnect=False, connect_timeout=1,
                              max_reconnect_attempts=1, error_cb=ignore_error)


async def wait_replicas(nodes, *, fully_current):
    names = {node.name for node in nodes}
    async with asyncio.timeout(30):
        while True:
            connection = None
            try:
                connection = await connect_oracle(nodes[0])
                js = connection.jetstream(timeout=1)
                stream = await js.stream_info(STREAM)
                consumer = await js.consumer_info(STREAM, DURABLE)
                if all(info.cluster and info.cluster.leader in names and len(info.cluster.replicas or []) == 2
                       for info in (stream, consumer)):
                    if not fully_current or all(peer.current and not peer.offline
                                                for info in (stream, consumer) for peer in info.cluster.replicas):
                        return stream, consumer
            except (nats.errors.Error, OSError, TimeoutError):
                pass
            finally:
                if connection is not None:
                    await connection.close()
            await asyncio.sleep(0.15)


async def run_client(primary, peer, counts, *args, fault=None):
    env = {**os.environ, "MOONNATS_CLUSTER_PRIMARY": primary.url,
           "MOONNATS_CLUSTER_PEER": peer.url, **counts}
    process = await asyncio.create_subprocess_exec(str(CLIENT), *args, cwd=ROOT, env=env,
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    fault_seen = False

    async def pump(stream):
        nonlocal fault_seen
        while line := await stream.readline():
            line = line.decode(errors="replace").rstrip()
            print(line, flush=True)
            if line == "STOP_STREAM_LEADER":
                if fault is None or fault_seen:
                    raise RuntimeError("unexpected leader stop request")
                fault_seen = True
                await fault()
                process.stdin.write(b"resume\n")
                await process.stdin.drain()

    try:
        async with asyncio.timeout(300):
            await asyncio.gather(pump(process.stdout), pump(process.stderr))
            if await process.wait() != 0:
                raise RuntimeError(f"MoonBit cluster client exited with status {process.returncode}")
            if fault is not None and not fault_seen:
                raise RuntimeError("stream leader fault was never exercised")
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


async def verify_final(nodes, jobs):
    expected = 2 * jobs + 2
    for node in nodes:
        report = await asyncio.to_thread(node.monitor, "jsz?streams=true")
        local = next(stream for account in report["account_details"]
                     for stream in account.get("stream_detail", []) if stream["name"] == STREAM)
        assert local["state"]["messages"] == local["state"]["last_seq"] == expected
        nc = await connect_oracle(node)
        try:
            js = nc.jetstream(timeout=3)
            stream = await js.stream_info(STREAM)
            consumer = await js.consumer_info(STREAM, DURABLE)
            assert stream.config.num_replicas == 3
            assert stream.state.messages == stream.state.last_seq == expected
            assert consumer.num_pending == consumer.num_ack_pending == 0
            assert consumer.ack_floor.stream_seq == expected
            first = (await js.get_msg(STREAM, seq=1)).data
            last_batch = (await js.get_msg(STREAM, seq=expected - 1)).data
            assert len(first) == len(last_batch) == 1024
            assert 0 <= int.from_bytes(first[:4], "big") < jobs
            assert jobs <= int.from_bytes(last_batch[:4], "big") < jobs * 2
            assert first[4:] == last_batch[4:] == bytes(i % 251 for i in range(4, 1024))
            assert (await js.get_msg(STREAM, seq=jobs + 1)).data == b"in-flight"
            assert (await js.get_msg(STREAM, seq=expected)).data == b"after-roll"
        finally:
            await nc.close()
    print(json.dumps({"phase": "final-oracle", "nodes": len(nodes), "replicas": 3,
                      "stored_messages": expected, "ack_floor": expected,
                      "pending": 0, "ack_pending": 0}), flush=True)


async def exercise(nodes, core_count, job_count):
    counts = {"MOONNATS_CLUSTER_CORE_COUNT": str(core_count),
              "MOONNATS_CLUSTER_JOB_COUNT": str(job_count)}
    for node in nodes:
        node.configure(nodes)
        node.start()
    await wait_routes(nodes)
    await run_client(nodes[0], nodes[1], counts, "--prepare")
    initial, _ = await wait_replicas(nodes, fully_current=True)
    leader = next(node for node in nodes if node.name == initial.cluster.leader)
    survivors = [node for node in nodes if node is not leader]

    async def stop_leader():
        await leader.stop(crash=True)
        await wait_routes(survivors)
        elected, _ = await wait_replicas(survivors, fully_current=False)
        assert elected.cluster.leader != leader.name
        print(f"stream leader {leader.name} lost, elected {elected.cluster.leader}", flush=True)

    # The MoonBit client receives only the leader as a seed. It must discover
    # at least one survivor through NATS INFO to reconnect after the crash.
    await run_client(leader, survivors[0], counts, fault=stop_leader)
    leader.start()
    await wait_routes(nodes)
    _, consumer = await wait_replicas(nodes, fully_current=True)
    roll = next(node for node in nodes if node.name == consumer.cluster.leader)
    survivors = [node for node in nodes if node is not roll]
    await roll.stop()
    await wait_routes(survivors)
    _, elected_consumer = await wait_replicas(survivors, fully_current=False)
    assert elected_consumer.cluster.leader != roll.name
    print(f"consumer leader {roll.name} rolled, elected {elected_consumer.cluster.leader}", flush=True)
    await run_client(*survivors, counts, "--after-roll")
    roll.start()
    await wait_routes(nodes)
    await wait_replicas(nodes, fully_current=True)
    await verify_final(nodes, job_count)


async def main(args):
    path = ROOT / ".test-data"
    path.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="cluster-", dir=path) as directory:
        ports = allocate_ports()
        nodes = [Node(f"mn{index+1}", ports[index*3:index*3+3], Path(directory), args.server)
                 for index in range(3)]
        try:
            await exercise(nodes, args.core_count, args.jobs_per_phase)
        except BaseException:
            for node in nodes:
                if node.log.exists():
                    print(f"{node.name} log tail:\n{node.log.read_text(encoding='utf-8', errors='replace')[-3000:]}", flush=True)
            raise
        finally:
            await asyncio.gather(*(node.stop() for node in nodes), return_exceptions=True)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", type=Path)
    parser.add_argument("--core-count", type=int, default=50000)
    parser.add_argument("--jobs-per-phase", type=int, default=6000)
    args = parser.parse_args()
    if not 1 <= args.core_count <= 1000000 or not 128 <= args.jobs_per_phase <= 1000000:
        parser.error("load counts must be within 1–1000000, with at least 128 jobs per phase")
    args.server = args.server.resolve() if args.server else ROOT / ".tools/nats-server" / (
        "nats-server.exe" if os.name == "nt" else "nats-server")
    subprocess.run(["moon", "build", "cmd/cluster_integration", "--target", "native"],
                   cwd=ROOT, check=True, timeout=90)
    asyncio.run(main(args))
