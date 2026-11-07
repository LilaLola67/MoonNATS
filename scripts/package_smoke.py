"""Install the published-format archive as a dependency of a fresh module."""
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import zipfile


ROOT = Path(__file__).resolve().parent.parent


def main():
    subprocess.run(["moon", "package", "--list"], cwd=ROOT, check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=90)
    manifest = (ROOT / "moon.mod").read_text(encoding="utf-8")
    version = re.search(r'^version = "([^"]+)"$', manifest, re.MULTILINE).group(1)
    archive = ROOT / "_build" / "publish" / f"LilaLola67-moonnats-{version}.zip"
    with tempfile.TemporaryDirectory(prefix="moonnats-package-", dir=ROOT / "_build") as directory:
        sandbox = Path(directory)
        dependency = sandbox / "dependency"
        with zipfile.ZipFile(archive) as package:
            for item in package.infolist():
                if not (dependency / item.filename).resolve().is_relative_to(dependency.resolve()):
                    raise ValueError("package archive contains a path outside its root")
            package.extractall(dependency)
        consumer = sandbox / "consumer"
        consumer.mkdir()
        (sandbox / "moon.work").write_text(
            'members = ["consumer", "dependency"]\n', encoding="utf-8")
        (consumer / "moon.mod").write_text(
            f'name = "smoke/moonnats-consumer"\nimport {{ "LilaLola67/moonnats@{version}" }}\n',
            encoding="utf-8")
        for example in ("pubsub", "tasks"):
            shutil.copytree(dependency / "examples" / example, consumer / example)
        env = {**os.environ, "MOONNATS_URL": "nats://127.0.0.1:14222"}
        subprocess.run(["moon", "check", "--target", "native", "--deny-warn"],
                       cwd=consumer, env=env, check=True, timeout=90)
        for example in ("pubsub", "tasks"):
            subprocess.run(["moon", "run", example, "--target", "native"],
                           cwd=consumer, env=env, check=True, timeout=90)
    print("package smoke: fresh MoonBit module imported the archive and ran Core and JetStream examples PASS")


if __name__ == "__main__":
    main()
