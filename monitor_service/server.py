"""Loopback-only local API and dashboard. Not an Internet deployment server."""
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import urlsplit,parse_qs
from pathlib import Path
from datetime import datetime
import json,threading,time,uuid,logging,math
from .store import Store,Conflict,encode
from .bench import Bench
from .log_worker import LogWorkers, reason_text
from .log_ingest import LogIngestError

class Server(ThreadingHTTPServer):
    daemon_threads=True
    def __init__(self,address,store,log_directory=None):
        if address[0] not in ('127.0.0.1','localhost','::1'):raise ValueError('Первая версия запускается только на этом компьютере.')
        super().__init__(address,Handler)
        self.store=store;self.stop_event=threading.Event();self.worker_lock=threading.Lock();self.jobs={}
        self.logs=LogWorkers(store,log_directory or store.path.parent/'log_inputs')
        self.bench=Bench(store,access_directory=self.logs.directory,access_log_guard=self.guard_log_checks,access_profile='owned_access_us_v2')
    def close(self):
        self.stop_event.set();self.logs.close();self.bench.close();self.shutdown();self.server_close()
    def log_model_for(self,filename):
        matches=[source for source in self.logs.sources() if source['filename']==filename]
        if not matches:return None
        source=matches[-1]
        return {'source':source['source'],'ready':bool(source['model'] and source['model']['ready']),
                'samples':source['model']['samples'] if source['model'] else 0,
                'required':896,'running':source['worker']['running'],'status':source['status']}
    def guard_log_checks(self,filename):
        model=self.log_model_for(filename)
        if model and not (model['ready'] and model['running'] and model['status']=='active'):
            raise ValueError('Сначала завершите настройку модели журнала и устраните паузу чтения. Проверки сбоев пока заблокированы.')
    def bench_state(self):
        state=self.bench.state();current=state['current']
        state['access_log_monitor']=self.log_model_for(current['access_log_file']) if current and current.get('access_log_file') else None
        return state
    def log_state(self):
        state=self.logs.state()
        with self.bench.lock:
            current=self.bench.current
            state['application_log']=({'filename':current['access_log_file'],'run':current['id'],'profile':current.get('access_log_profile','nginx_access_v1'),
                                       'running':current['running'] and not self.bench.stop_event.is_set()}
                                      if current and current.get('access_log_file') else None)
        return state
    def local_collector(self):
        from .collector import metrics
        with self.worker_lock:
            if self.jobs.get('local',{}).get('running'):return {'id':'local-pc','already_running':True}
            probe=metrics();sample=probe.sample()
            existing=self.store.state()
            if any(h['id']=='local-pc' for h in existing):
                old=next(h for h in existing if h['id']=='local-pc')
                if old['features']!=list(sample):raise ValueError('Состав местных показателей изменился; нужен новый идентификатор сервера.')
            else:self.store.register({'id':'local-pc','name':'Этот компьютер','source':'local','features':list(sample)})
            job={'id':'local-pc','running':True,'error':None};self.jobs['local']=job
            def work():
                try:
                    while not self.stop_event.wait(1):
                        self.store.ingest('local-pc',{'observations':[{'timestamp':time.time(),'values':probe.sample()}]})
                except Exception as exc:job['error']=str(exc);logging.exception('Local collector stopped')
                finally:job['running']=False
            threading.Thread(target=work,daemon=True,name='local-metrics').start()
            return {'id':'local-pc','already_running':False}
    def demo(self):
        import numpy as np
        with self.worker_lock:
            for job in self.jobs.values():
                if job.get('kind')=='demo' and job['running']:return {'id':job['id'],'already_running':True}
            host='demo-'+uuid.uuid4().hex[:8]
            features=['cpu','memory','disk_read','disk_write','network_in','network_out']
            self.store.register({'id':host,'name':'Демонстрационный поток','source':'demo','features':features,'settings':{'components':2}})
            job={'id':host,'kind':'demo','running':True,'error':None};self.jobs[host]=job
            def work():
                try:
                    rng=np.random.default_rng(4187)
                    total=1280;latent=rng.normal(size=(total,2))
                    for i in range(1,total):latent[i]=.8*latent[i-1]+.6*latent[i]
                    loading=np.array([[1,.6,.8,.3,.7,.2],[.1,.8,.2,.9,.3,.8]])
                    raw=latent@loading+.08*rng.normal(size=(total,6))
                    raw[1000:1008,0]+=8
                    raw[1160:1184,2:4]+=6
                    origin=time.time()-total*.05
                    for a in range(0,total,16):
                        if self.stop_event.is_set():break
                        rows=[{'timestamp':origin+i*.05,'values':dict(zip(features,raw[i].tolist()))} for i in range(a,a+16)]
                        self.store.ingest(host,{'observations':rows})
                        if self.stop_event.wait(.04 if a<896 else .3):break
                except Exception as exc:job['error']=str(exc);logging.exception('Demo stopped')
                finally:job['running']=False
            threading.Thread(target=work,daemon=True,name='demo-replay').start()
            return {'id':host,'already_running':False}

class Handler(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    def setup(self):
        super().setup();self.connection.settimeout(15)
    def log_message(self,fmt,*args):logging.info('%s %s',self.address_string(),fmt%args)
    def send(self,status,data,content_type='application/json; charset=utf-8'):
        content=data if isinstance(data,bytes) else encode(data).encode('utf-8')
        self.send_response(status);self.send_header('Content-Type',content_type)
        self.send_header('Content-Length',str(len(content)));self.send_header('Cache-Control','no-store')
        self.send_header('X-Content-Type-Options','nosniff');self.send_header('X-Frame-Options','DENY')
        self.send_header('Content-Security-Policy',"default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'")
        self.end_headers();self.wfile.write(content)
    def do_GET(self):self.handle_request(False)
    def do_POST(self):self.handle_request(True)
    def handle_request(self,post):
        try:
            # Reject cross-site browser requests; no CORS and no GET mutations.
            host=self.headers.get('Host','')
            if host not in (f'127.0.0.1:{self.server.server_port}',f'localhost:{self.server.server_port}'):
                self.send(403,{'error':'Недопустимый адрес запроса.'});return
            origin=self.headers.get('Origin')
            if origin and origin not in ('http://'+host,):self.send(403,{'error':'Запрос с другого сайта отклонён.'});return
            path=urlsplit(self.path);parts=path.path.strip('/').split('/')
            data={}
            if post:
                if self.headers.get_content_type()!='application/json':raise ValueError('Нужен Content-Type application/json.')
                size=int(self.headers.get('Content-Length','0'))
                if not 0<size<=512*1024:
                    self.close_connection=True;self.send(413,{'error':'Размер запроса: от 1 до 524288 байт.'});return
                data=json.loads(self.rfile.read(size),parse_constant=lambda x:(_ for _ in ()).throw(ValueError('Неконечное число.')))
                if not isinstance(data,dict):raise ValueError('Тело запроса должно быть JSON-объектом.')
            store=self.server.store
            if not post and path.path=='/api/health':self.send(200,{'status':'ok','version':'0.7.0','delivery_rows':16,'default_delivery_rows':16,'application_delivery_rows':1,'model_update_rows':16,'log_reading':True,'application_access_log':True,'precise_log_quality':True});return
            if not post and path.path=='/api/state':
                with self.server.worker_lock:jobs=[dict(v) for v in self.server.jobs.values()]
                self.send(200,{'hosts':store.state(),'jobs':jobs,'log_sources':self.server.logs.sources()});return
            if not post and path.path=='/api/logs':self.send(200,self.server.log_state());return
            if post and path.path=='/api/logs/start':self.send(202,self.server.logs.start(data));return
            if post and path.path=='/api/logs/stop':
                if set(data)!={'source_id'} or type(data['source_id']) is not str:raise ValueError('Выберите одно наблюдение за журналом.')
                self.send(200,self.server.logs.stop(data['source_id']));return
            if post and path.path=='/api/hosts':self.send(201,store.register(data));return
            if post and len(parts)==4 and parts[:2]==['api','hosts'] and parts[3]=='observations':self.send(200,store.ingest(parts[2],data));return
            if not post and len(parts)==4 and parts[:2]==['api','hosts'] and parts[3]=='history':
                limit=int(parse_qs(path.query).get('limit',['160'])[0]);self.send(200,store.history(parts[2],limit));return
            if post and path.path=='/api/local/start':self.send(202,self.server.local_collector());return
            if post and path.path=='/api/demo':self.send(202,self.server.demo());return
            if not post and path.path=='/api/bench':self.send(200,self.server.bench_state());return
            if not post and path.path=='/api/bench/status':self.send(200,self.server.bench.service_status(parse_qs(path.query).get('run',[None])[0]));return
            if post and path.path=='/api/bench/settings':self.send(200,self.server.bench.policy.set(data));return
            if post and path.path=='/api/bench/start':self.send(202,self.server.bench.start());return
            if post and path.path=='/api/bench/stop':self.send(202,self.server.bench.stop());return
            if post and path.path=='/api/bench/control':self.send(200,self.server.bench.control(data.get('mode'),data.get('seconds',10)));return
            if post and path.path=='/api/bench/load':self.send(200,self.server.bench.set_load(data.get('rate'),data.get('seconds',10)));return
            if not post and path.path in ('/','/app.js','/bench.js','/status.js','/logs.js','/style.css'):
                name='index.html' if path.path=='/' else path.path[1:]
                suffix={'index.html':'text/html; charset=utf-8','app.js':'application/javascript; charset=utf-8','bench.js':'application/javascript; charset=utf-8','status.js':'application/javascript; charset=utf-8','logs.js':'application/javascript; charset=utf-8','style.css':'text/css; charset=utf-8'}
                self.send(200,(Path(__file__).parent/'static'/name).read_bytes(),suffix[name]);return
            self.send(404,{'error':'Адрес не найден.'})
        except Conflict as exc:self.send(409,{'error':str(exc)})
        except LogIngestError as exc:self.send(409,{'error':reason_text(exc.code)})
        except KeyError as exc:self.send(404,{'error':str(exc)})
        except (ValueError,TypeError,OverflowError) as exc:
            self.close_connection=True;self.send(400,{'error':str(exc)})
        except (BrokenPipeError,ConnectionResetError):pass
        except Exception:
            logging.exception('Request failed');self.send(500,{'error':'Ошибка обработки. Подробности сохранены в журнале сервиса.'})

