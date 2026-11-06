"""Real-server proxy injecting fragmentation and one Lame Duck INFO notification."""
import socket, subprocess, threading, queue, time
from pathlib import Path
root=Path(__file__).resolve().parent.parent
listener=socket.socket();listener.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
listener.bind(('127.0.0.1',14229));listener.listen();listener.settimeout(.2)
stop=threading.Event();connections=[];writers=[];threads=[];guard=threading.Lock()
def forward(source,target,lock,fragment=False):
    try:
        while not stop.is_set():
            data=source.recv(65536)
            if not data: break
            with lock:
                if fragment:
                    for start in range(0,len(data),127): target.sendall(data[start:start+127])
                else: target.sendall(data)
    except OSError: pass
    finally:
        try: target.shutdown(socket.SHUT_RDWR)
        except OSError: pass
def accept():
    while not stop.is_set():
        try: client,_=listener.accept()
        except socket.timeout: continue
        except OSError: break
        upstream=socket.create_connection(('127.0.0.1',14222),timeout=5);upstream.settimeout(None)
        writer=threading.Lock()
        with guard: connections.extend([client,upstream]);writers.append((client,writer))
        for source,target,lock,fragment in [(client,upstream,threading.Lock(),False),(upstream,client,writer,True)]:
            thread=threading.Thread(target=forward,args=(source,target,lock,fragment),daemon=True);threads.append(thread);thread.start()
thread=threading.Thread(target=accept,daemon=True);thread.start()
process=None
try:
    executable=root/'_build/native/debug/build/cmd/fault_integration/fault_integration.exe'
    process=subprocess.Popen([str(executable)],cwd=root,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    lines=queue.Queue()
    def read():
        for line in process.stderr: lines.put(line)
    reader=threading.Thread(target=read,daemon=True);reader.start()
    deadline=time.monotonic()+15
    while process.poll() is None:
        if time.monotonic()>deadline: raise TimeoutError('Fault proxy test timed out')
        try: line=lines.get(timeout=.1)
        except queue.Empty: continue
        if line.strip()=='LDM':
            with guard: client,writer=writers[0]
            with writer: client.sendall(b'INFO {"ldm":true,"max_payload":1048576}\r\n')
        else: print(line,end='')
    reader.join(timeout=1)
    while not lines.empty(): print(lines.get(),end='')
    print(process.stdout.read(),end='')
    if process.returncode: raise RuntimeError('Fault client exited '+str(process.returncode))
finally:
    if process and process.poll() is None: process.kill();process.wait()
    stop.set();listener.close()
    for conn in connections:
        try: conn.shutdown(socket.SHUT_RDWR)
        except OSError: pass
        conn.close()
    thread.join(timeout=1)
    for thread in threads: thread.join(timeout=1)
