"""Transactional raw observations, decisions and model snapshots in SQLite."""
import json,math,re,sqlite3,threading,time
from datetime import datetime,timezone
from pathlib import Path
from contextlib import contextmanager
from .detector import Detector
from .immediate import ImmediateDetector, MODE, restore_detector

def utc():return datetime.now(timezone.utc).isoformat()
def encode(obj):return json.dumps(obj,ensure_ascii=False,allow_nan=False,separators=(',',':'))
class Conflict(ValueError):pass

class Store:
    def __init__(self,path):
        self.path=Path(path);self.path.parent.mkdir(parents=True,exist_ok=True)
        self.lock=threading.RLock()
        with self.connection() as db:
            db.executescript('''PRAGMA journal_mode=WAL;
              CREATE TABLE IF NOT EXISTS hosts(id TEXT PRIMARY KEY,name TEXT NOT NULL,source TEXT NOT NULL,state TEXT NOT NULL,last_ts REAL,created TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS observations(host TEXT NOT NULL,ts REAL NOT NULL,received TEXT NOT NULL,payload TEXT NOT NULL,result TEXT,PRIMARY KEY(host,ts));
              CREATE TABLE IF NOT EXISTS notifications(id INTEGER PRIMARY KEY,host TEXT NOT NULL,source_end REAL NOT NULL,emitted TEXT NOT NULL,details TEXT NOT NULL);
              CREATE INDEX IF NOT EXISTS observation_host ON observations(host,ts DESC);
              CREATE TABLE IF NOT EXISTS log_messages(host TEXT NOT NULL,ts REAL NOT NULL,kind TEXT NOT NULL,
                title TEXT NOT NULL,message TEXT NOT NULL,details TEXT NOT NULL,PRIMARY KEY(host,ts));
            ''')
    @contextmanager
    def connection(self):
        db=sqlite3.connect(self.path,timeout=30)
        db.row_factory=sqlite3.Row
        try:
            with db:yield db
        finally:db.close()
    def register(self,data):
        host=data.get('id');features=data.get('features');name=data.get('name',host)
        if not isinstance(host,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}',host):raise ValueError('Название сервера: 1–64 латинских буквы, цифры, дефис или подчёркивание.')
        if not isinstance(name,str) or not 1<=len(name)<=160:raise ValueError('Некорректное отображаемое имя.')
        if not isinstance(features,list) or not 2<=len(features)<=64 or any(not isinstance(f,str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}',f) for f in features) or len(set(features))!=len(features):
            raise ValueError('Нужны 2–64 разных названия числовых показателей.')
        source=data.get('source','external')
        if source not in ('external','local','demo','application'):raise ValueError('Неизвестный источник.')
        allowed={'components','fit_rows','reference_rows','validation_rows','processing_mode','handler_limit_ms','allowed_error_fraction'}
        settings=data.get('settings',{})
        if not isinstance(settings,dict) or set(settings)-allowed:raise ValueError('Неизвестные настройки модели.')
        settings=dict(settings)
        mode=settings.pop('processing_mode','batch16')
        if mode=='log_quality_v2':
            from .log_detector import LogDetector
            detector=LogDetector(features,**settings)
        else:
            if mode not in ('batch16',MODE):raise ValueError('Неизвестный режим обработки данных.')
            if set(settings)&{'handler_limit_ms','allowed_error_fraction'}:raise ValueError('Параметры качества доступны для новой модели журнала.')
            detector=(ImmediateDetector if mode==MODE else Detector)(features,**settings)
        with self.lock,self.connection() as db:
            if db.execute('SELECT id FROM hosts WHERE id=?',(host,)).fetchone():raise Conflict('Сервер уже зарегистрирован.')
            if db.execute('SELECT count(*) FROM hosts').fetchone()[0]>=32:raise ValueError('Локальная версия поддерживает до 32 записей серверов.')
            db.execute('INSERT INTO hosts VALUES(?,?,?,?,?,?)',(host,name,source,encode(detector.to_state()),None,utc()))
        return {'id':host,'warmup_required':detector.required,'delivery_rows':1 if mode in (MODE,'log_quality_v2') else 16,
                'processing_mode':mode,'model_update_rows':16}

    def ingest(self,host,data):
        with self.lock,self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            # A bound log model may only be advanced by its atomic window delivery.
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='log_sources'").fetchone():
                if db.execute('SELECT 1 FROM log_sources WHERE host=?',(host,)).fetchone():
                    raise Conflict('Эта модель получает данные только из подключённого журнала.')
            result=self._ingest_in_transaction(db,host,data)
            started=result.pop('_processing_started')
        result.update(storage_committed_at=utc(),processing_seconds=time.perf_counter()-started)
        return result

    def _ingest_in_transaction(self,db,host,data):
        """Caller owns the lock and transaction; no commit or shared model cache."""
        if not db.in_transaction:raise RuntimeError('Нужна внешняя транзакция приёма данных.')
        rows=data.get('observations')
        if not isinstance(rows,list) or not 1<=len(rows)<=256:raise ValueError('Передайте от 1 до 256 наблюдений.')
        info=db.execute('SELECT * FROM hosts WHERE id=?',(host,)).fetchone()
        if info is None:raise KeyError('Сервер не найден.')
        detector=restore_detector(json.loads(info['state']))
        previous=info['last_ts'];validated=[]
        for row in rows:
            if not isinstance(row,dict):raise ValueError('Наблюдение должно быть объектом.')
            ts=row.get('timestamp')
            if type(ts) not in (int,float) or not 0<ts<1e11 or not math.isfinite(ts):raise ValueError('Время — число секунд Unix, конечное и положительное.')
            if previous is not None and ts<=previous:raise Conflict('Наблюдения должны идти строго по времени, без повторов.')
            values=row.get('values')
            if not isinstance(values,dict) or set(values)!=set(detector.features):raise ValueError('Состав показателей должен совпадать с настройкой сервера; пропуски не заполняются.')
            ordered=[values[f] for f in detector.features]
            if any(type(x) not in (int,float) or abs(x)>1e12 or not math.isfinite(x) for x in ordered):raise ValueError('Показатели должны быть конечными числами по модулю не больше 10¹².')
            validated.append((ts,values,ordered));previous=ts
        received=utc();decisions=[];notifications=0;started=time.perf_counter()
        # All input validation occurs before any detector/DB mutation.
        # Detector is reconstructed locally: failed transaction cannot poison memory.
        for ts,values,ordered in validated:
            result=detector.observe(ts,ordered)
            db.execute('INSERT INTO observations VALUES(?,?,?,?,NULL)',(host,ts,received,encode(values)))
            if result:
                emitted=utc()
                for decision in result:
                    decision['emitted_at']=emitted
                    db.execute('UPDATE observations SET result=? WHERE host=? AND ts=?',(encode(decision),host,decision['timestamp']))
                    if 'message_kind' in decision:
                        db.execute('INSERT INTO log_messages VALUES(?,?,?,?,?,?)',
                            (host,decision['timestamp'],decision['message_kind'],decision['message_title'],decision['message'],encode(decision)))
                alarms=[r for r in result if r['alarm']]
                if alarms:
                    db.execute('INSERT INTO notifications(host,source_end,emitted,details) VALUES(?,?,?,?)',
                       (host,result[-1]['decision_source_time'],emitted,encode({'alarm_observations':len(alarms),'largest_residual_features':alarms[-1]['largest_residual_features']})))
                    notifications+=1
                decisions.extend(result)
        db.execute('UPDATE hosts SET state=?,last_ts=? WHERE id=?',(encode(detector.to_state()),previous,host))
        return {'accepted':len(rows),'decisions':decisions,'notifications':notifications,'_processing_started':started,
           'phase':getattr(detector,'phase','monitoring' if detector.ready else 'warmup'),'samples':detector.seen,
           'pending':len(detector.pending),'warmup_required':detector.required}

    def state(self,host=None):
        with self.lock,self.connection() as db:
            hosts=db.execute('SELECT * FROM hosts'+(' WHERE id=?' if host else '')+' ORDER BY created', (host,) if host else ()).fetchall()
            if host and not hosts:raise KeyError('Сервер не найден.')
            result=[]
            for row in hosts:
                s=json.loads(row['state'])
                result.append({'id':row['id'],'name':row['name'],'source':row['source'],
                    'processing_mode':s.get('processing_mode','batch16'),
                    'delivery_rows':1 if s.get('processing_mode') in (MODE,'log_quality_v2') else 16,'model_update_rows':16,
                    'notification_unit':'observation' if s.get('processing_mode') in (MODE,'log_quality_v2') else 'batch',
                    'phase':s.get('phase','monitoring' if s['ready'] else 'warmup'),'samples':s['seen'],'pending':len(s.get('pending',s.get('core',{}).get('pending',[]))),
                    'warmup_required':sum(s['lengths']),'features':s['features'],'components':s['config']['components'],
                    'last_timestamp':row['last_ts'],'notifications':db.execute('SELECT count(*) FROM notifications WHERE host=?',(row['id'],)).fetchone()[0],
                    'log_settings':s.get('log_settings'), 'update_counts':s.get('candidate',{}).get('update_counts')})
        return result
    def history(self,host,limit=160):
        if not 1<=limit<=512:raise ValueError('Лимит от 1 до 512.')
        with self.lock,self.connection() as db:
            if not db.execute('SELECT id FROM hosts WHERE id=?',(host,)).fetchone():raise KeyError('Сервер не найден.')
            rows=db.execute('SELECT * FROM observations WHERE host=? ORDER BY ts DESC LIMIT ?',(host,limit)).fetchall()
            notes=db.execute('SELECT * FROM notifications WHERE host=? ORDER BY id DESC LIMIT 30',(host,)).fetchall()
        return {'observations':[{'timestamp':r['ts'],'received_at':r['received'],'values':json.loads(r['payload']),
                'decision':json.loads(r['result']) if r['result'] else None} for r in reversed(rows)],
            'notifications':[{'id':r['id'],'emitted_at':r['emitted'],'source_end':r['source_end'],**json.loads(r['details'])} for r in notes]}

    def log_messages(self,host,limit=30,events_only=False):
        if type(limit) is not int or not 1<=limit<=256:raise ValueError('Неверный предел сообщений.')
        with self.connection() as db:
            rows=db.execute('SELECT ts,kind,title,message,details FROM log_messages WHERE host=? '+
                ("AND kind NOT IN ('normal','training','recovering') " if events_only else '')+
                'ORDER BY ts DESC LIMIT ?',(host,limit)).fetchall()
        return [dict(timestamp=r['ts'],kind=r['kind'],title=r['title'],message=r['message'],details=json.loads(r['details'])) for r in rows]
