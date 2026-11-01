"""Small owned loopback application with bounded, expiring test problems.

No host-wide load, memory exhaustion, system changes or external destinations.
Client-measured latency/status are held-out outcomes, not detector inputs.
"""
import argparse,ctypes,hashlib,json,os,secrets,threading,time
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import urlsplit
from .process_cycles import ProcessCycles, validate_bracket
from .log_access_writer import AccessLogWriter

EXTRA_CPU_SECONDS=.020
EXTRA_WALL_SECONDS=.100

def extra_work(keep_running,cpu_seconds=EXTRA_CPU_SECONDS,wall_seconds=EXTRA_WALL_SECONDS):
    """Discarded computation, bounded by thread CPU and wall time.

    Clock resolution can overshoot the CPU target; actual time/iterations are
    recorded. The wall cap and cancellation are checked between 32-hash blocks.
    """
    start_cpu=time.thread_time();start_wall=time.perf_counter();iterations=0
    value=b'owned-extra-work'
    while keep_running() and time.thread_time()-start_cpu<cpu_seconds and time.perf_counter()-start_wall<wall_seconds:
        for _ in range(32):value=hashlib.sha256(value).digest()
        iterations+=32
    return {'cpu_seconds':max(0.,time.thread_time()-start_cpu),
            'wall_seconds':max(0.,time.perf_counter()-start_wall),'iterations':iterations}

def memory_mib():
    if os.name=='nt':
        from ctypes import wintypes as w
        class Counters(ctypes.Structure):
            _fields_=[('cb',w.DWORD),('faults',w.DWORD)]+[(k,ctypes.c_size_t) for k in
              ('peak_working','working','peak_paged','paged','peak_nonpaged','nonpaged','pagefile','peak_pagefile','private')]
        kernel=ctypes.WinDLL('kernel32',use_last_error=True)
        psapi=ctypes.WinDLL('psapi',use_last_error=True)
        kernel.GetCurrentProcess.restype=w.HANDLE
        psapi.GetProcessMemoryInfo.argtypes=[w.HANDLE,ctypes.POINTER(Counters),w.DWORD]
        psapi.GetProcessMemoryInfo.restype=w.BOOL
        result=Counters();result.cb=ctypes.sizeof(result)
        if not psapi.GetProcessMemoryInfo(kernel.GetCurrentProcess(),ctypes.byref(result),result.cb):raise ctypes.WinError(ctypes.get_last_error())
        return result.working/1048576
    resident=int(Path('/proc/self/statm').read_text().split()[1])
    return resident*os.sysconf('SC_PAGE_SIZE')/1048576

class ApplicationState:
    def __init__(self,delay_seconds=.25,cycle_reader=None):
        self.lock=threading.Lock();self.started=0;self.completed=0;self.active=0;self.bytes_sent=0
        self.extra_cpu_seconds=0.;self.extra_work_requests=0;self.extra_work_iterations=0
        self.mode='normal';self.expires=0.;self.delay_seconds=delay_seconds;self.lease=time.monotonic()
        self.cycle_reader_error=None
        try:self.cycle_reader=ProcessCycles() if cycle_reader is None else cycle_reader
        except (OSError,AttributeError) as exc:
            self.cycle_reader=None;self.cycle_reader_error=type(exc).__name__+': '+str(exc)
    def set_mode(self,mode,seconds):
        if mode not in ('normal','delay','error','extra_cpu'):raise ValueError('Неизвестный режим приложения.')
        if type(seconds) not in (int,float) or not 1<=seconds<=120:raise ValueError('Длительность: от 1 до 120 секунд.')
        with self.lock:self.mode=mode;self.expires=time.monotonic()+seconds
    def current_mode(self):
        with self.lock:
            if time.monotonic()>=self.expires:self.mode='normal'
            return self.mode
    def metrics(self):
        # RSS is a gauge sampled just before the locked counter snapshot.
        memory=memory_mib()
        with self.lock:
            started=time.perf_counter_ns();before=after=None;error=self.cycle_reader_error
            status='unsupported' if self.cycle_reader is not None and not self.cycle_reader.available else 'error'
            if self.cycle_reader is not None and self.cycle_reader.available:
                try:before=self.cycle_reader.read()
                except (OSError,ValueError,TypeError) as exc:error=type(exc).__name__+': '+str(exc)
            snapshot={'timestamp':time.time(),'cpu_seconds':time.process_time(),'memory_mib':memory,
               'requests_started':self.started,'requests_completed':self.completed,
               'active_requests':self.active,'response_bytes':self.bytes_sent,
               'extra_cpu_seconds':self.extra_cpu_seconds,'extra_work_requests':self.extra_work_requests,
               'extra_work_iterations':self.extra_work_iterations}
            if self.cycle_reader is not None and self.cycle_reader.available and error is None:
                try:
                    after=self.cycle_reader.read();validate_bracket(before,after);status='available'
                except (OSError,ValueError,TypeError) as exc:error=type(exc).__name__+': '+str(exc)
            if status!='available':before=after=None
            snapshot.update(metrics_schema='application_cycles_snapshot_v1',
                            cpu_cycles=before,cpu_cycles_after_snapshot=after,
                            cpu_cycles_read_span_ns=time.perf_counter_ns()-started,
                            cpu_cycles_status=status,cpu_cycles_error=error,
                            cpu_cycles_source='QueryProcessCycleTime',cpu_cycles_unit='cycles')
            return snapshot

class Application(ThreadingHTTPServer):
    daemon_threads=True
    def __init__(self,address=('127.0.0.1',0),token=None,lease_seconds=30,access_log=None,access_profile='nginx_access_v1'):
        if address[0]!='127.0.0.1':raise ValueError('Учебное приложение запускается только локально.')
        super().__init__(address,ApplicationHandler)
        self.state=ApplicationState();self.token=token or secrets.token_urlsafe(32)
        self.slots=threading.BoundedSemaphore(4);self.done=threading.Event();self.lease_seconds=lease_seconds
        try:self.access_log=AccessLogWriter(access_log,profile=access_profile) if access_log is not None else None
        except Exception:self.server_close();raise
        if self.access_log:self.daemon_threads=False  # Drain request handlers before closing their log.
    def close(self):
        self.done.set();self.shutdown();self.server_close()
    def server_close(self):
        super().server_close()
        writer=getattr(self,'access_log',None)
        if writer:writer.close()

class ApplicationHandler(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    def setup(self):super().setup();self.connection.settimeout(5)
    def log_message(self,*args):pass
    def send(self,status,data,mode=None,work=None):
        content=json.dumps(data,allow_nan=False).encode()
        self.send_response(status);self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(content)));self.send_header('Cache-Control','no-store')
        if mode:self.send_header('X-Workbench-Mode',mode)
        if work is not None:
            self.send_header('X-Workbench-Extra-CPU-Ms',str(work['cpu_seconds']*1000))
            self.send_header('X-Workbench-Extra-Wall-Ms',str(work['wall_seconds']*1000))
            self.send_header('X-Workbench-Extra-Iterations',str(work['iterations']))
        self.send_header('Connection','close');self.end_headers();self.close_connection=True
        self.wfile.write(content)
        return len(content)
    def authorized(self):return secrets.compare_digest(self.headers.get('Authorization',''),'Bearer '+self.server.token)
    def allowed_host(self):
        return self.headers.get('Host')==f'127.0.0.1:{self.server.server_port}'
    def do_GET(self):
        if not self.allowed_host():self.send(403,{'error':'host'});return
        path=urlsplit(self.path).path
        if path=='/health':self.send(200,{'status':'ok','role':'owned-test-application'});return
        if path=='/metrics':
            if self.authorized():self.server.state.lease=time.monotonic()
            snapshot=self.server.state.metrics()
            if self.server.access_log:snapshot['access_log']=self.server.access_log.diagnostics()
            self.send(200,snapshot);return
        if path!='/work':self.send(404,{'error':'not_found'});return
        if self.server.access_log is None:self.work_request();return
        started=time.perf_counter_ns();status=0
        try:
            status=self.work_request()
            self.wfile.flush()
        except (BrokenPipeError,ConnectionResetError,TimeoutError):status=0
        finally:self.server.access_log.record(status,time.perf_counter_ns()-started)
    def work_request(self):
        if not self.server.slots.acquire(blocking=False):self.send(429,{'error':'capacity'});return 429
        state=self.server.state
        with state.lock:state.started+=1;state.active+=1
        size=0;status=0;work={'cpu_seconds':0.,'wall_seconds':0.,'iterations':0}
        try:
            mode=state.current_mode()
            if mode=='delay':time.sleep(state.delay_seconds)
            if mode=='error':size=self.send(503,{'error':'controlled-test-error'},mode);status=503
            else:
                if mode=='extra_cpu':
                    work=extra_work(lambda:not self.server.done.is_set() and state.current_mode()=='extra_cpu')
                # A small, fixed task. Not a stress or saturation generator.
                value=hashlib.sha256(b'thesis-workbench').digest()
                for _ in range(128):value=hashlib.sha256(value).digest()
                size=self.send(200,{'result':value.hex()},mode,work);status=200
        except (BrokenPipeError,ConnectionResetError,TimeoutError):pass
        finally:
            with state.lock:
                state.completed+=1;state.active-=1;state.bytes_sent+=size
                state.extra_cpu_seconds+=work['cpu_seconds'];state.extra_work_iterations+=work['iterations']
                if work['iterations']:state.extra_work_requests+=1
            self.server.slots.release()
        return status
    def do_POST(self):
        self.close_connection=True
        if not self.allowed_host() or not self.authorized():self.send(403,{'error':'forbidden'});return
        if self.headers.get('Origin'):self.send(403,{'error':'browser_control_forbidden'});return
        try:
            size=int(self.headers.get('Content-Length','0'))
            if not 0<size<=1024:raise ValueError('Недопустимый размер команды.')
            data=json.loads(self.rfile.read(size))
            if urlsplit(self.path).path=='/control':
                self.server.state.set_mode(data['mode'],data.get('seconds',10))
                self.send(200,{'mode':self.server.state.current_mode(),'maximum_seconds':data.get('seconds',10)})
            elif urlsplit(self.path).path=='/shutdown':
                self.send(200,{'stopping':True})
                threading.Thread(target=self.server.shutdown,daemon=True).start()
            else:self.send(404,{'error':'not_found'})
        except (ValueError,TypeError,KeyError):self.send(400,{'error':'invalid_control'})

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--port',type=int,default=8766)
    parser.add_argument('--ready-file',type=Path,required=True)
    parser.add_argument('--access-log',type=Path)
    parser.add_argument('--access-profile',choices=('nginx_access_v1','owned_access_us_v2'),default='nginx_access_v1');args=parser.parse_args()
    token=os.environ.get('THESIS_TEST_APP_TOKEN')
    if not token:raise SystemExit('Application must be started by the monitoring workbench.')
    app=Application(('127.0.0.1',args.port),token,access_log=args.access_log,access_profile=args.access_profile)
    args.ready_file.parent.mkdir(parents=True,exist_ok=True)
    temporary=args.ready_file.with_suffix('.tmp')
    temporary.write_text(json.dumps({'pid':os.getpid(),'port':app.server_port,'role':'owned-test-application'}),encoding='utf-8')
    temporary.replace(args.ready_file)
    def watchdog():
        while not app.done.wait(1):
            if time.monotonic()-app.state.lease>app.lease_seconds:
                app.shutdown();return
    threading.Thread(target=watchdog,daemon=True).start()
    try:app.serve_forever()
    except KeyboardInterrupt:pass
    finally:app.done.set();app.server_close()
if __name__=='__main__':main()
