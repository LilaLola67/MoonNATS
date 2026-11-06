"""Download a pinned official server and verify its published SHA-256."""
from pathlib import Path
import hashlib, platform, tarfile, subprocess, zipfile
VERSION='2.15.2'
root=Path(__file__).resolve().parent.parent
out=root/'.tools/nats-server'
out.mkdir(parents=True, exist_ok=True)
os_name={'Windows':'windows','Linux':'linux','Darwin':'darwin'}[platform.system()]
arch={'AMD64':'amd64','x86_64':'amd64','arm64':'arm64','aarch64':'arm64'}[platform.machine()]
filename=f'nats-server-v{VERSION}-{os_name}-{arch}'+('.zip' if os_name=='windows' else '.tar.gz')
base=f'https://github.com/nats-io/nats-server/releases/download/v{VERSION}/'
archive=root/'.tools'/filename
subprocess.run(["curl.exe" if os_name=="windows" else "curl","-fsSL","--retry","3",base+filename,"-o",str(archive)],check=True,timeout=120)
checksum_file=root/".tools/SHA256SUMS"
subprocess.run(["curl.exe" if os_name=="windows" else "curl","-fsSL",base+"SHA256SUMS","-o",str(checksum_file)],check=True,timeout=30)
checksums=checksum_file.read_text()
expected=next(line.split()[0] for line in checksums.splitlines() if line.split()[-1].lstrip('*')==filename)
actual=hashlib.sha256(archive.read_bytes()).hexdigest()
if actual!=expected: raise RuntimeError('Official server checksum mismatch')
if os_name=='windows':
    with zipfile.ZipFile(archive) as z:
        member=next(n for n in z.namelist() if n.endswith('/nats-server.exe'))
        (out/'nats-server.exe').write_bytes(z.read(member))
else:
    with tarfile.open(archive) as t:
        member=next(m for m in t if m.name.endswith('/nats-server'))
        (out/'nats-server').write_bytes(t.extractfile(member).read())
    (out/'nats-server').chmod(0o755)
print(f'Official NATS Server {VERSION}: SHA-256 verified')
