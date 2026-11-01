"""Opt-in v0.5: thread-local urllib openers; fresh connection for each request.

No shared opener across workers, socket pool, retries, model or schedule changes.
Old v0.4 remains unchanged. This module never starts collection on import.
"""
import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import time
import urllib.error
import urllib.request

from .queued_cycle_collection import QueuedCycleBench, CAPACITY
from .cycle_collection import seal_collection
from .efficiency_pilot import collect as base_collect, write_json
from .efficiency_schedule import preview

VERSION='worker-opener-collector-v0.5'


class WorkerOpeners:
    """Each calling thread constructs and exclusively owns one default opener.

    urllib HTTPHandler still sends Connection: close, so requests never reuse
    a socket. last_event is thread local; statistics are bounded by worker count.
    """
    def __init__(self, factory=None):
        self.factory=factory or (lambda:urllib.request.build_opener(urllib.request.ProxyHandler({})))
        self.local=threading.local();self.lock=threading.Lock()
        self.records=[];self.failed_builds=0

    def open(self, request, timeout=2):
        started=time.perf_counter();cached=getattr(self.local,'cached',None)
        event=dict(cache_hit=cached is not None,build_ms=0.,error=None)
        self.local.last_event=event
        if cached is None:
            build_started=time.perf_counter()
            try:opener=self.factory()
            except BaseException:
                with self.lock:self.failed_builds+=1
                event['build_ms']=(time.perf_counter()-build_started)*1000
                event['error']='opener_build_failed'
                raise  # No retry of this request.
            event['build_ms']=(time.perf_counter()-build_started)*1000
            with self.lock:
                entry=dict(cache_id=len(self.records)+1,thread_ident=threading.get_ident(),
                           thread_name=threading.current_thread().name,
                           owner_object_id=id(threading.current_thread()),
                           build_ms=event['build_ms'],calls=0,errors=0)
                self.records.append(entry)
            cached=(opener,entry);self.local.cached=cached
        opener,entry=cached
        event['cache_id']=entry['cache_id']
        event['prepare_ms']=(time.perf_counter()-started)*1000
        with self.lock:entry['calls']+=1
        open_started=time.perf_counter()
        try:return opener.open(request,timeout=timeout)
        except BaseException as exc:
            event['error']=type(exc).__name__
            with self.lock:entry['errors']+=1
            raise
        finally:event['open_to_headers_ms']=(time.perf_counter()-open_started)*1000

    def last_event(self):
        return dict(getattr(self.local,'last_event',{}))

    def snapshot(self):
        with self.lock:
            records=[dict(r) for r in self.records]
            return dict(version=VERSION,opener_count=len(records),failed_builds=self.failed_builds,
                calls=sum(r['calls'] for r in records),errors=sum(r['errors'] for r in records),
                records=records,fresh_connection_per_request=True,shared_between_threads=False,
                automatic_retries=False)


class CachedCycleBench(QueuedCycleBench):
    def __init__(self, store):
        super().__init__(store);self.http_client=WorkerOpeners()

    def start(self):
        run=super().start()
        with self.journal.db() as db:
            config=json.loads(db.execute('SELECT config FROM runs WHERE id=?',(run['id'],)).fetchone()[0])
            config['http_client']=dict(version=VERSION,opener_scope='one per active HTTP worker',
                fresh_connection_per_request=True,default_handlers_retained=True,proxies=False,
                automatic_retries=False,max_workers=2)
            db.execute('UPDATE runs SET config=? WHERE id=?',(json.dumps(config),run['id']))
        return run

    def acquire_response(self, run):
        sent=time.time();clock=time.perf_counter();status=error=mode=work=fingerprint=None
        try:
            with self.http_client.open(self.url+'/work',timeout=2) as response:
                status=response.status;mode=response.headers.get('X-Workbench-Mode');body=response.read()
                fingerprint=hashlib.sha256(body).hexdigest()
                work=dict(cpu_ms=float(response.headers['X-Workbench-Extra-CPU-Ms']),
                          wall_ms=float(response.headers['X-Workbench-Extra-Wall-Ms']),
                          iterations=int(response.headers['X-Workbench-Extra-Iterations']))
        except Exception as exc:
            error=type(exc).__name__
            # Preserve v0.4 error fields; explicitly close an HTTP error response.
            if isinstance(exc,urllib.error.HTTPError):exc.close()
        return dict(run=run,sent=sent,received=time.time(),duration_ms=(time.perf_counter()-clock)*1000,
                    status=status,error=error,mode=mode,work=work,body_sha256=fingerprint)

    def close(self):
        super().close()
        if self.current:
            client=self.http_client.snapshot()
            owners=[r['owner_object_id'] for r in client['records']]
            client['accounting_complete']=(client['calls']==self.submitted and client['failed_builds']==0
                and client['opener_count']<=2 and len(set(owners))==len(owners))
            path=self.root/'collector_closed.json';closed=json.loads(path.read_text(encoding='utf-8'))
            closed['http_client']=client;write_json(path,closed)
            if not client['accounting_complete']:raise RuntimeError('Worker HTTP opener accounting failed.')


def collect(output, order):
    output=Path(output).resolve()
    result=base_collect(output,order,profile='cycles_v1',bench_factory=CachedCycleBench)
    closed=json.loads((output/'testbench/collector_closed.json').read_text(encoding='utf-8'))
    if not closed['all_submitted_results_persisted'] or not closed['http_client']['accounting_complete']:
        raise ValueError('Only complete persistence and worker accounting can be sealed.')
    with closing(sqlite3.connect((output/'testbench/application.sqlite3').as_uri()+'?mode=ro',uri=True)) as original:
        with closing(sqlite3.connect(output/'application_snapshot.sqlite3')) as destination:
            original.backup(destination);destination.execute('PRAGMA journal_mode=DELETE')
    seal_collection(output,result['run']);return result


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--collect',action='store_true')
    parser.add_argument('--order',type=int,choices=(1,2),default=1);parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    if not args.collect:
        print(json.dumps(dict(preview(args.order),collector=VERSION,collection_started=False)));return
    if args.output is None:parser.error('--output required')
    collect(args.output,args.order)


if __name__=='__main__':main()
