"""Stop a TLS server and verify the plaintext fallback is never dialed."""
from pathlib import Path
import queue
import socket
import subprocess
import sys
import threading
import time


ROOT = Path(__file__).resolve().parent.parent


def main(server_binary):
    original = (ROOT / ".tools/auth.local.conf").read_text(encoding="utf-8")
    config = ROOT / ".tools/tls-policy.local.conf"
    config.write_text(original.replace("14223", "14230"), encoding="utf-8")
    executable = ROOT / "_build/native/debug/build/cmd/auth_integration/auth_integration.exe"
    with socket.socket() as plain, (ROOT / ".tools/tls-policy.log").open("w") as log:
        plain.bind(("127.0.0.1", 14231))
        plain.listen()
        plain.setblocking(False)
        server = subprocess.Popen([server_binary, "-c", str(config)], cwd=ROOT,
                                  stdout=log, stderr=log)
        client = None
        try:
            time.sleep(0.4)
            if server.poll() is not None:
                raise RuntimeError("TLS policy test server failed")
            client = subprocess.Popen([str(executable), "--tls-policy"], cwd=ROOT,
                                      stderr=subprocess.PIPE, text=True)
            lines = queue.Queue()
            reader = threading.Thread(target=lambda: lines.put(client.stderr.readline()), daemon=True)
            reader.start()
            if lines.get(timeout=8).strip() != "TLS_READY":
                raise RuntimeError("TLS client did not finish its secure handshake")
            server.terminate()
            server.wait(timeout=5)
            if client.wait(timeout=8) != 0:
                raise RuntimeError(client.stderr.read())
            try:
                connection, _ = plain.accept()
            except BlockingIOError:
                pass
            else:
                connection.close()
                raise AssertionError("secure client dialed a plaintext fallback")
        finally:
            for process in (client, server):
                if process is not None and process.poll() is None:
                    process.terminate()
                    process.wait(timeout=5)
    print("TLS policy: verified TLS connection and no dial to plaintext fallback after outage PASS")


if __name__ == "__main__":
    main(sys.argv[1])
