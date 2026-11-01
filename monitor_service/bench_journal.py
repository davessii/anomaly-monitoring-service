"""Separate application outcomes and controls: never detector input labels."""
from pathlib import Path
import sqlite3,json,time
from contextlib import contextmanager

class Journal:
    def __init__(self,path):
        self.path=Path(path);self.path.parent.mkdir(parents=True,exist_ok=True)
        with self.db() as db:
            db.executescript('''PRAGMA journal_mode=WAL;
              CREATE TABLE IF NOT EXISTS runs(id TEXT PRIMARY KEY,host TEXT NOT NULL,started REAL NOT NULL,ended REAL,status TEXT NOT NULL,error TEXT,config TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS requests(id INTEGER PRIMARY KEY,run TEXT NOT NULL,sent REAL NOT NULL,received REAL NOT NULL,duration_ms REAL NOT NULL,status INTEGER,error TEXT,served_mode TEXT);
              CREATE TABLE IF NOT EXISTS samples(id INTEGER PRIMARY KEY,run TEXT NOT NULL,ts REAL NOT NULL,counters TEXT NOT NULL,detector_values TEXT NOT NULL,ingest_returned REAL NOT NULL,decision_count INTEGER NOT NULL);
              CREATE TABLE IF NOT EXISTS controls(id INTEGER PRIMARY KEY,run TEXT NOT NULL,requested REAL NOT NULL,mode TEXT NOT NULL,duration_seconds REAL NOT NULL);
              CREATE TABLE IF NOT EXISTS request_work(request_id INTEGER PRIMARY KEY,cpu_ms REAL NOT NULL,wall_ms REAL NOT NULL,iterations INTEGER NOT NULL);
              CREATE TABLE IF NOT EXISTS load_changes(id INTEGER PRIMARY KEY,run TEXT NOT NULL,requested REAL NOT NULL,rate REAL NOT NULL,duration_seconds REAL NOT NULL);
              CREATE INDEX IF NOT EXISTS requests_run ON requests(run,id);
              CREATE INDEX IF NOT EXISTS samples_run ON samples(run,id);
            ''')
            db.execute("UPDATE runs SET status='interrupted',ended=?,error='Monitoring process restarted; run not resumed.' WHERE status='running'",(time.time(),))
    @contextmanager
    def db(self):
        conn=sqlite3.connect(self.path,timeout=3);conn.row_factory=sqlite3.Row
        try:
            with conn:yield conn
        finally:conn.close()
    def start(self,run,host,config):
        with self.db() as db:db.execute('INSERT INTO runs VALUES(?,?,?,NULL,?,NULL,?)',(run,host,time.time(),'running',json.dumps(config)))
    def finish(self,run,status,error=None):
        with self.db() as db:db.execute('UPDATE runs SET ended=?,status=?,error=? WHERE id=?',(time.time(),status,error,run))
    def request(self,run,sent,received,duration_ms,status,error,mode,work=None):
        with self.db() as db:
            record=db.execute('INSERT INTO requests(run,sent,received,duration_ms,status,error,served_mode) VALUES(?,?,?,?,?,?,?)',(run,sent,received,duration_ms,status,error,mode))
            if work is not None:db.execute('INSERT INTO request_work VALUES(?,?,?,?)',(record.lastrowid,work['cpu_ms'],work['wall_ms'],work['iterations']))
    def load_change(self,run,rate,seconds):
        with self.db() as db:db.execute('INSERT INTO load_changes(run,requested,rate,duration_seconds) VALUES(?,?,?,?)',(run,time.time(),rate,seconds))
    def sample(self,run,counters,values,decisions):
        with self.db() as db:db.execute('INSERT INTO samples(run,ts,counters,detector_values,ingest_returned,decision_count) VALUES(?,?,?,?,?,?)',
            (run,counters['timestamp'],json.dumps(counters,allow_nan=False),json.dumps(values,allow_nan=False),time.time(),decisions))
    def control(self,run,mode,seconds):
        with self.db() as db:db.execute('INSERT INTO controls(run,requested,mode,duration_seconds) VALUES(?,?,?,?)',(run,time.time(),mode,seconds))
    def runs(self):
        with self.db() as db:
            runs=[dict(r) for r in db.execute('SELECT * FROM runs ORDER BY started DESC LIMIT 20')]
            for run in runs:
                run['config']=json.loads(run['config'])
                recent=[dict(r) for r in db.execute('SELECT r.sent,r.received,r.duration_ms,r.status,r.error,r.served_mode,w.cpu_ms AS extra_cpu_ms,w.wall_ms AS extra_wall_ms,w.iterations AS extra_iterations FROM requests r LEFT JOIN request_work w ON w.request_id=r.id WHERE r.run=? ORDER BY r.id DESC LIMIT 60',(run['id'],))]
                recent.reverse();run['recent_requests']=recent
                run['controls']=[dict(r) for r in db.execute('SELECT requested,mode,duration_seconds FROM controls WHERE run=? ORDER BY id DESC LIMIT 10',(run['id'],))]
                run['load_changes']=[dict(r) for r in db.execute('SELECT requested,rate,duration_seconds FROM load_changes WHERE run=? ORDER BY id DESC LIMIT 10',(run['id'],))]
                run['request_count']=db.execute('SELECT count(*) FROM requests WHERE run=?',(run['id'],)).fetchone()[0]
                run['error_responses']=db.execute('SELECT count(*) FROM requests WHERE run=? AND (status IS NULL OR status>=400)',(run['id'],)).fetchone()[0]
                run['sample_count']=db.execute('SELECT count(*) FROM samples WHERE run=?',(run['id'],)).fetchone()[0]
        return runs
