"""Owned child application, small workload, genuine metrics, separate outcomes."""
import os,sys,json,time,secrets,threading,subprocess,urllib.request,urllib.error,uuid,logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from .bench_journal import Journal
from .immediate import MODE
from .service_status import ServicePolicy,build_status
from .detector import np

FEATURES=('app_cpu_percent','app_memory_mib','active_requests','completed_per_second')
LOAD_RATES=(2,4,8)
def open_direct(request,timeout=3):
    return urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request,timeout=timeout)
def get_json(url,token=None):
    req=urllib.request.Request(url,headers={} if token is None else {'Authorization':'Bearer '+token})
    with open_direct(req) as response:return json.load(response)
def command(url,token,body):
    req=urllib.request.Request(url,data=json.dumps(body).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
    with open_direct(req) as response:return json.load(response)
def detector_values(previous,current):
    dt=current['timestamp']-previous['timestamp']
    if dt<=0:raise ValueError('Application measurement time did not advance.')
    return {'app_cpu_percent':max(0.,(current['cpu_seconds']-previous['cpu_seconds'])/dt*100),
       'app_memory_mib':current['memory_mib'],'active_requests':float(current['active_requests']),
       'completed_per_second':max(0.,(current['requests_completed']-previous['requests_completed'])/dt)}

class Bench:
    features=FEATURES
    input_profile='cpu_percent_v1'
    def derive_values(self,previous,current):return detector_values(previous,current)
    def record_anchor(self,run,raw):pass
    def stamp_measurement(self,raw):pass
    def __init__(self,store,access_directory=None,access_log_guard=None,access_profile='nginx_access_v1'):
        self.store=store;self.root=store.path.parent/'testbench';self.root.mkdir(exist_ok=True)
        self.journal=Journal(self.root/'application.sqlite3');self.lock=threading.RLock()
        self.policy=ServicePolicy(self.root/'service_policy.json')
        self.stop_event=threading.Event();self.thread=None;self.process=None;self.output=None
        self.current=None;self.mode='normal';self.mode_until=0.;self.closing=False
        self.load_rate=2.;self.load_until=0.;self.scheduled=0;self.submitted=0;self.skipped=0
        self.access_directory=Path(access_directory).resolve() if access_directory is not None else None
        if access_profile not in ('nginx_access_v1','owned_access_us_v2'):raise ValueError('Unknown log profile')
        self.access_profile=access_profile
        if self.access_directory:self.access_directory.mkdir(parents=True,exist_ok=True)
        self.access_log_path=None;self.access_log_status=None;self.access_log_guard=access_log_guard
    def spawn(self):
        self.token=secrets.token_urlsafe(32)
        self.ready_file=self.root/(self.current['id']+'-ready.json')
        self.output=(self.root/(self.current['id']+'-application.log')).open('ab')
        env=os.environ.copy();env['THESIS_TEST_APP_TOKEN']=self.token
        args=[sys.executable,'-B','-m','monitor_service.test_app','--port','0','--ready-file',str(self.ready_file.resolve())]
        if self.access_log_path:args+=['--access-log',str(self.access_log_path),'--access-profile',self.access_profile]
        self.process=subprocess.Popen(args,cwd=Path(__file__).resolve().parents[1],env=env,
           stdin=subprocess.DEVNULL,stdout=self.output,stderr=subprocess.STDOUT,
           creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        deadline=time.monotonic()+12
        while time.monotonic()<deadline:
            if self.process.poll() is not None:raise RuntimeError('Учебное приложение завершилось при запуске. Подробности в его журнале.')
            if self.ready_file.exists():
                ready=json.loads(self.ready_file.read_text(encoding='utf-8'))
                if ready['pid']!=self.process.pid:raise RuntimeError('Не совпал идентификатор учебного процесса.')
                self.url='http://127.0.0.1:'+str(ready['port'])
                if get_json(self.url+'/health')['role']!='owned-test-application':raise RuntimeError('Неизвестное приложение по адресу.')
                return
            time.sleep(.05)
        raise RuntimeError('Учебное приложение не успело запуститься.')
    def start(self):
        with self.lock:
            if self.closing:raise ValueError('Сервис останавливается.')
            if self.thread and self.thread.is_alive():return dict(self.current)
            if self.process and self.process.poll() is None:raise ValueError('Предыдущее приложение ещё завершается.')
            run='app-'+uuid.uuid4().hex[:10]
            self.current={'id':run,'host':run,'running':True,'error':None,'maximum_seconds':1800,'started_at':time.time()}
            self.access_log_path=self.access_directory/(run+'-access.log') if self.access_directory else None
            self.access_log_status=None
            if self.access_log_path:self.current.update(access_log_file=self.access_log_path.name,access_log_profile=self.access_profile)
            self.stop_event=threading.Event();self.mode='normal';self.mode_until=0
            self.load_rate=2.;self.load_until=0.;self.scheduled=self.submitted=self.skipped=0
            self.journal.start(run,run,{'version':('0.7.0' if self.access_profile=='owned_access_us_v2' else '0.6.1') if self.access_log_path else '0.5.0','features':self.features,'input_profile':self.input_profile,'components':2,'sample_interval_seconds':1,
                'processing_mode':MODE,'score_every_rows':1,'model_update_rows':16,
                'load_intervals_seconds':[.37,.61,.43,.59],'max_load_workers':2,'maximum_seconds':1800,
                'available_load_rps':LOAD_RATES,'extra_cpu_target_ms':20,'extra_work_wall_cap_ms':100,
                'runtime':{'python':sys.version.split()[0],'numpy':np.__version__,'numpy_path':str(np.__file__),
                           'thread_environment':{k:os.environ.get(k) for k in ('OPENBLAS_NUM_THREADS','OPENBLAS_DEFAULT_NUM_THREADS','OMP_NUM_THREADS','MKL_NUM_THREADS')},
                           'active_blas_threads_verified':False},
                'outcomes_excluded_from_detector':['response_time_ms','http_status','served_mode','control_mode','extra_cpu_seconds','extra_work_requests','extra_work_iterations','extra_cpu_ms','extra_wall_ms','extra_iterations','client_target_rps','client_schedule_attempts','client_submitted_requests','client_skipped_slots'],
                'scope':'Development workbench; no scientific superiority test.',
                **({'access_log':{'profile':self.access_profile,'filename':self.access_log_path.name,
                    'producer':'owned_application','included_requests':['GET /work'],
                    'duration_scope':'application_handler_to_response_flush','precision':'integer_us_floor' if self.access_profile=='owned_access_us_v2' else 'integer_ms_floor',
                    'unknown_response_status':'000 (unsupported by detector; pauses log model)',
                    'separate_model_required':True}} if self.access_log_path else {})})
            try:
                self.spawn()
                self.store.register({'id':run,'name':'Учебное приложение · '+run[-6:],'source':'application',
                    'features':list(self.features),'settings':{'components':2,'processing_mode':MODE}})
            except Exception as exc:
                self.current.update(running=False,error=str(exc));self.journal.finish(run,'failed',str(exc));self.cleanup_child();raise
            self.thread=threading.Thread(target=self.work,daemon=True,name='application-workbench');self.thread.start()
            return dict(self.current)
    def one_request(self,run):
        sent=time.time();clock=time.perf_counter();status=None;error=None;mode=None;work=None
        try:
            with open_direct(self.url+'/work',timeout=2) as response:
                status=response.status;mode=response.headers.get('X-Workbench-Mode');response.read()
                if response.headers.get('X-Workbench-Extra-CPU-Ms') is not None:
                    work={'cpu_ms':float(response.headers['X-Workbench-Extra-CPU-Ms']),
                          'wall_ms':float(response.headers['X-Workbench-Extra-Wall-Ms']),
                          'iterations':int(response.headers['X-Workbench-Extra-Iterations'])}
        except urllib.error.HTTPError as exc:
            status=exc.code;mode=exc.headers.get('X-Workbench-Mode');exc.read();exc.close()
        except Exception as exc:error=type(exc).__name__
        duration=(time.perf_counter()-clock)*1000;received=time.time()
        self.journal.request(run,sent,received,duration,status,error,mode,work)
    def workload(self,run):
        intervals=(.37,.61,.43,.59);index=0
        # At most two outstanding requests; no unbounded submission queue.
        slots=threading.BoundedSemaphore(2)
        def completed(future):
            try:future.result()
            except Exception as exc:
                if self.current and self.current['id']==run:
                    self.current['error']='Не удалось сохранить запрос: '+str(exc);self.stop_event.set()
            finally:slots.release()
        with ThreadPoolExecutor(max_workers=2) as pool:
            while not self.stop_event.is_set():
                rate=self.effective_load()
                with self.lock:
                    self.scheduled+=1;admitted=slots.acquire(blocking=False)
                    if admitted:self.submitted+=1
                    else:self.skipped+=1
                if admitted:pool.submit(self.one_request,run).add_done_callback(completed)
                if self.stop_event.wait(intervals[index%4]*2/rate):break
                index+=1
    def work(self):
        run=self.current['id'];error=None
        workload=threading.Thread(target=self.workload,args=(run,),daemon=True,name='small-app-workload')
        try:
            previous=get_json(self.url+'/metrics',self.token)
            self.check_access_log(previous)
            self.record_anchor(run,previous)
            workload.start();deadline=time.monotonic()+1800
            while not self.stop_event.wait(1):
                if time.monotonic()>=deadline:break
                current=get_json(self.url+'/metrics',self.token)
                self.check_access_log(current)
                self.stamp_measurement(current)
                with self.lock:
                    current.update(client_target_rps=self.effective_load(),client_schedule_attempts=self.scheduled,
                                   client_submitted_requests=self.submitted,client_skipped_slots=self.skipped)
                values=self.derive_values(previous,current)
                result=self.store.ingest(run,{'observations':[{'timestamp':current['timestamp'],'values':values}]})
                self.journal.sample(run,current,values,len(result['decisions']))
                previous=current
        except Exception as exc:error=str(exc);logging.exception('Application workbench stopped')
        finally:
            self.stop_event.set()
            if workload.is_alive():workload.join(timeout=6)
            self.cleanup_child()
            with self.lock:
                error=error or self.current.get('error');self.mode='normal';self.mode_until=0;self.load_rate=2.;self.load_until=0.
                self.current.update(running=False,error=error,ended_at=time.time())
                self.journal.finish(run,'failed' if error else 'stopped',error)
    def check_access_log(self,snapshot):
        if self.access_log_path is None:return
        info=snapshot.get('access_log')
        if (not isinstance(info,dict) or info.get('profile')!=self.access_profile or
            info.get('filename')!=self.access_log_path.name or info.get('error') or info.get('closed')):
            raise ValueError('Учебное приложение не смогло продолжить запись журнала запросов.')
        with self.lock:self.access_log_status=dict(info)
    def control(self,mode,seconds=10):
        with self.lock:
            if not self.current or not self.current['running'] or self.stop_event.is_set():raise ValueError('Сначала запустите учебное приложение.')
            if mode not in ('normal','delay','error','extra_cpu'):raise ValueError('Неизвестная проверка.')
            if type(seconds) not in (int,float) or not 1<=seconds<=120:raise ValueError('Длительность: от 1 до 120 секунд.')
            if mode!='normal' and not self.store.state(self.current['host'])[0]['phase']=='monitoring':
                raise ValueError('Сначала дождитесь начального обучения приложения. Проблемы во время него не включаются.')
            if mode!='normal' and self.access_log_path and self.access_log_guard:
                self.access_log_guard(self.access_log_path.name)
            if mode=='extra_cpu' and self.effective_load()!=2:
                raise ValueError('Сначала верните обычный поток запросов: лишние вычисления проверяются отдельно.')
            result=command(self.url+'/control',self.token,{'mode':mode,'seconds':seconds})
            self.journal.control(self.current['id'],mode,seconds)
            self.mode=mode;self.mode_until=time.time()+seconds if mode!='normal' else 0
            if mode=='normal':self.set_load(2,10)
            return result
    def effective_load(self):
        with self.lock:return self.load_rate if time.monotonic()<self.load_until else 2.
    def set_load(self,rate,seconds=10):
        with self.lock:
            if not self.current or not self.current['running'] or self.stop_event.is_set():raise ValueError('Сначала запустите учебное приложение.')
            if type(rate) not in (int,float) or rate not in LOAD_RATES:raise ValueError('Частота запросов: 2, 4 или 8 в секунду.')
            if type(seconds) not in (int,float) or not 1<=seconds<=120:raise ValueError('Длительность: от 1 до 120 секунд.')
            if rate>2 and self.mode=='extra_cpu' and time.time()<self.mode_until:
                raise ValueError('Лишние вычисления включены. Сначала верните обычную работу приложения.')
            self.journal.load_change(self.current['id'],rate,seconds)
            self.load_rate=float(rate);self.load_until=time.monotonic()+seconds if rate!=2 else 0.
            return {'rate':float(rate),'maximum_seconds':seconds,'simultaneous_requests_limit':2}
    def stop(self):
        # Cancellation is asynchronous so a browser does not wait on worker joins.
        with self.lock:
            self.stop_event.set()
            return {'stopping':True}
    def cleanup_child(self):
        process=self.process
        if process and process.poll() is None:
            try:command(self.url+'/shutdown',self.token,{})
            except Exception:pass
            try:process.wait(timeout=4)
            except subprocess.TimeoutExpired:
                process.terminate() # Only the child owned by this Popen handle.
                try:process.wait(timeout=3)
                except subprocess.TimeoutExpired:process.kill();process.wait(timeout=3)
        if self.output:self.output.close();self.output=None
    def close(self):
        with self.lock:self.closing=True;self.stop_event.set();worker=self.thread
        if worker and worker.is_alive():worker.join(timeout=12)
        if not worker:self.cleanup_child()
    def state(self):
        with self.lock:
            current=dict(self.current) if self.current else None
            if current:
                current['mode']=self.mode if time.time()<self.mode_until else 'normal'
                current['mode_until']=self.mode_until
                current['stopping']=self.stop_event.is_set() and current['running']
                current['target_requests_per_second']=self.effective_load()
                current['load_seconds_left']=max(0.,self.load_until-time.monotonic()) if self.effective_load()!=2 else 0.
                current['scheduled_requests']=self.scheduled;current['submitted_requests']=self.submitted;current['skipped_slots']=self.skipped
                if self.access_log_path:current['access_log_status']=self.access_log_status
        runs=self.journal.runs()
        for run in runs:
            try:run['monitor']=self.store.state(run['host'])[0]
            except KeyError:run['monitor']=None
        return {'current':current,'runs':runs,'service_policy':self.policy.get()}
    def service_status(self,run_id=None):
        with self.journal.db() as db:
            run=db.execute('SELECT * FROM runs WHERE id=?',(run_id,)).fetchone() if run_id else db.execute('SELECT * FROM runs ORDER BY started DESC LIMIT 1').fetchone()
            if run is None:
                if run_id:raise KeyError('Запуск приложения не найден.')
                return None
            run=dict(run)
            requests=[dict(r) for r in db.execute('SELECT * FROM requests WHERE run=? ORDER BY received,id',(run['id'],))]
        with self.store.connection() as db:
            host=db.execute('SELECT state FROM hosts WHERE id=?',(run['host'],)).fetchone()
            mode=json.loads(host['state']).get('processing_mode') if host else None
            observations=[dict(r) for r in db.execute('SELECT ts,result FROM observations WHERE host=? AND result IS NOT NULL ORDER BY ts',(run['host'],))]
        return build_status(run,requests,observations,self.policy.get(),time.time(),1 if mode==MODE else 16)
