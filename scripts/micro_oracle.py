"""Probe MoonNATS's standard micro discovery API using the official Python client."""
import asyncio,json,subprocess,queue,threading
from pathlib import Path
import nats
root=Path(__file__).resolve().parent.parent
process=subprocess.Popen([str(root/'_build/native/debug/build/cmd/oracle/oracle.exe'),'--micro'],cwd=root,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
lines=queue.Queue()
def read():
    for line in process.stderr: lines.put(line.strip())
thread=threading.Thread(target=read,daemon=True);thread.start()
async def verify():
    nc=await nats.connect('nats://127.0.0.1:14222')
    try:
        assert (await nc.request('oracle.micro.echo',b'python-to-moonbit')).data==b'python-to-moonbit'
        ping=json.loads((await nc.request('$SRV.PING.OracleEcho',b'')).data)
        assert ping['type']=='io.nats.micro.v1.ping_response' and ping['version']=='1.0.0'
        info=json.loads((await nc.request('$SRV.INFO.OracleEcho.'+ping['id'],b'')).data)
        assert info['endpoints'][0]['subject']=='oracle.micro.echo'
        stats=json.loads((await nc.request('$SRV.STATS.OracleEcho',b'')).data)
        assert stats['endpoints'][0]['num_requests']==1
        assert stats['endpoints'][0]['num_errors']==0 and stats['started'].endswith('Z')
        await nc.publish('oracle.micro.stop',b'');await nc.flush()
    finally: await nc.close()
try:
    assert lines.get(timeout=10)=='READY'
    asyncio.run(verify())
    process.wait(timeout=10)
    assert process.returncode==0,process.stdout.read()
    print('oracle: official nats-py discovery, endpoint response and standard micro statistics PASS')
finally:
    if process.poll() is None: process.kill();process.wait()
    thread.join(timeout=1)
