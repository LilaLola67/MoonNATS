"""Run real-server, authentication, fault, CLI and independent-client tests."""
from pathlib import Path
import argparse, contextlib, os, subprocess, sys, time
root=Path(__file__).resolve().parent.parent
os.chdir(root)
parser=argparse.ArgumentParser();parser.add_argument('--server');args=parser.parse_args()
ext='.exe' if os.name=='nt' else ''
server=Path(args.server).resolve() if args.server else next((root/'.tools/nats-server').rglob('nats-server'+ext))
(root/'.tools').mkdir(exist_ok=True);(root/'.test-data').mkdir(exist_ok=True)
def run(*command):
    subprocess.run([str(x) for x in command],check=True,timeout=90,cwd=root)
def executable(package):
    # Moon's native backend uses .exe on Linux as well as Windows.
    return root/'_build/native/debug/build/cmd'/package/(package+'.exe')
@contextlib.contextmanager
def serving(name, config):
    path=root/'.tools'/(name+'.local.conf');path.write_text(config,encoding='utf-8')
    with open(root/'.tools'/(name+'.log'),'w') as log:
        process=subprocess.Popen([str(server),'-c',str(path)],stdout=log,stderr=log,cwd=root)
        try:
            time.sleep(.4)
            if process.poll() is not None: raise RuntimeError('Test server failed: '+name)
            yield
        finally:
            if process.poll() is None: process.terminate();process.wait(timeout=5)
for package in ['integration','oracle','recovery','auth_integration','moonnats','ws_integration','extended_integration','fault_integration','benchmark']:
    run('moon','build','cmd/'+package,'--target','native')
run('moon','build','cmd/ws_integration','--target','js')
run('moon','build','cmd/integration','--target','js')
run('moon','build','cmd/extended_integration','--target','js')
config='listen: "127.0.0.1:14222"\njetstream { store_dir: ".test-data/jetstream" }\nwebsocket { listen: "127.0.0.1:18080", no_tls: true }\n'
with serving('test',config):
    run(executable('integration'))
    run(executable('extended_integration'))
    run(sys.executable,'scripts/fault_proxy.py')
    # JavaScript uses the browser-standard WebSocket transport.
    os.environ['MOONNATS_TEST_URL']='ws://127.0.0.1:18080'
    run('node',root/'_build/js/debug/build/cmd/integration/integration.js')
    run('node',root/'_build/js/debug/build/cmd/extended_integration/extended_integration.js')
    del os.environ['MOONNATS_TEST_URL']
    run(executable('ws_integration'))
    run('node',root/'_build/js/debug/build/cmd/ws_integration/ws_integration.js')
    run(sys.executable,'scripts/micro_oracle.py')
    run(executable('benchmark'))
    run(executable('oracle'));run(sys.executable,'scripts/oracle.py');run(executable('oracle'),'--verify')
    os.environ['MOONNATS_URL']='nats://127.0.0.1:14222'
    for example in ['pubsub','request_reply','tasks','micro','workers']:
        run('moon','run','examples/'+example,'--target','native')
    env={**os.environ}
    del os.environ['MOONNATS_URL']
    result=subprocess.run([str(executable('moonnats')),'info'],env=env,capture_output=True,text=True,check=True,timeout=10)
    assert 'server_id' in result.stdout
    result=subprocess.run([str(executable('moonnats')),'metrics'],env=env,capture_output=True,text=True,check=True,timeout=10)
    assert 'moonnats_requests_total' in result.stdout
    result=subprocess.run([str(executable('moonnats')),'request','no.responder','hello'],env=env,capture_output=True,text=True,timeout=10)
    assert result.returncode==1 and 'error' in result.stderr
    print('CLI: info, nonzero error status and clean stderr PASS')
run(sys.executable,'scripts/recovery.py',server)
public=subprocess.check_output([str(executable('auth_integration')),'--public'],text=True).strip()
with open(root/'.tools/certificate.log','w') as cert_log:
    subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout','.test-data/key.pem','-out','.test-data/cert.pem','-days','2','-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost'],stdout=cert_log,stderr=cert_log,check=True,timeout=30)
auth='listen: "127.0.0.1:14223"\ntls { cert_file: ".test-data/cert.pem", key_file: ".test-data/key.pem" }\nauthorization { users: [{user: "test-user", password: "synthetic-test-password"}, {nkey: "'+public+'"}] }'
run('node','scripts/generate_jwt.cjs')
with serving('tls-first',auth.replace('14223','14228').replace('tls {','tls { handshake_first: true,')),serving('jwt',(root/'.tools/jwt.local.conf').read_text()),serving('auth',auth),serving('token','listen: "127.0.0.1:14224"\nauthorization { token: "synthetic-test-token" }'):
    run(executable('auth_integration'))
    env={**os.environ,'MOONNATS_URL':'nats://127.0.0.1:14227','MOONNATS_CREDS':'.test-data/user.creds'}
    result=subprocess.run([str(executable('moonnats')),'info'],env=env,capture_output=True,text=True,check=True,timeout=10)
    assert 'server_id' in result.stdout
print('All real-server integration suites passed')
