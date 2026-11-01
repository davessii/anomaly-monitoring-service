"""Opt-in collector v0.4: two HTTP workers, one bounded persistence writer.

Old collectors, model, load schedule and zero-skips rule are unchanged.
Successful closure requires all accepted results durably written and joined.
"""
import argparse
from contextlib import closing
import json
from pathlib import Path
import queue
import sqlite3
import threading
import time
from .cycle_collection import RecordedCycleBench,seal_collection
from .bench import open_direct
from .cycle_outputs import sha
from .efficiency_pilot import collect as base_collect,append_record,write_json
from .efficiency_schedule import preview
import hashlib

VERSION='queued-collector-v0.4'
CAPACITY=64


class ResultWriter:
    def __init__(self,sink,on_error,capacity=CAPACITY):
        if type(capacity) is not int or capacity<1:raise ValueError('Positive bounded capacity required.')
        self.queue=queue.Queue(maxsize=capacity);self.sink=sink;self.on_error=on_error
        self.lock=threading.RLock();self.closing=False;self.error=None
        self.accepted=self.persisted=self.rejected=self.peak=0;self.failed_record=None;self.rejected_records=[]
        self.thread=threading.Thread(target=self.work,name='bounded-result-writer',daemon=True);self.thread.start()

    def fail(self,exc):
        with self.lock:
            if self.error is None:self.error=type(exc).__name__+': '+str(exc)
        self.on_error(self.error)

    def submit(self,record):
        # Copy and validate BEFORE accepting; callers cannot mutate queued data.
        clean=json.loads(json.dumps(record,allow_nan=False))
        with self.lock:
            if self.closing or self.error:
                self.rejected+=1
                if len(self.rejected_records)<2:self.rejected_records.append(clean)
                raise RuntimeError('Writer closed or failed: '+str(self.error))
            try:
                self.queue.put_nowait(clean)
            except queue.Full:
                self.rejected+=1
                if len(self.rejected_records)<2:self.rejected_records.append(clean)
                exc=RuntimeError('Bounded persistence queue is full; collection invalid.')
            else:
                self.accepted+=1;self.peak=max(self.peak,self.queue.qsize());return
        self.fail(exc);raise exc

    def work(self):
        while True:
            with self.lock:
                if self.error:return
                closing=self.closing
            try:record=self.queue.get(timeout=.05)
            except queue.Empty:
                if closing:return
                continue
            try:
                self.sink(record)
                with self.lock:self.persisted+=1
            except BaseException as exc:
                with self.lock:self.failed_record=record
                self.fail(exc);return
            finally:self.queue.task_done()

    def finish(self,timeout=4):
        with self.lock:self.closing=True
        self.thread.join(timeout)
        if self.thread.is_alive():self.fail(RuntimeError('Persistence writer did not stop within timeout.'))
        result=self.status()
        if not result['complete']:raise RuntimeError('Persistence incomplete: '+str(result))
        return result

    def status(self):
        with self.lock:
            return dict(version=VERSION,capacity=self.queue.maxsize,accepted=self.accepted,persisted=self.persisted,
                rejected=self.rejected,peak_queued=self.peak,pending=self.queue.qsize(),writer_exited=not self.thread.is_alive(),
                error=self.error,complete=not self.thread.is_alive() and not self.error and
                self.rejected==0 and self.accepted==self.persisted and self.queue.empty())

    def incomplete_records(self):
        if self.thread.is_alive():return [] # Never race a live consumer; failure still explicit.
        with self.queue.mutex:pending=list(self.queue.queue)
        return ([self.failed_record] if self.failed_record else [])+pending+list(self.rejected_records)


class QueuedCycleBench(RecordedCycleBench):
    def __init__(self,store):super().__init__(store);self.writer=None;self.started_once=False

    def writer_error(self,message):
        with self.lock:
            if self.current:self.current['error']='Сохранение результатов: '+message
        self.stop_event.set()

    def persist(self,record):
        self.journal.request(record['run'],record['sent'],record['received'],record['duration_ms'],record['status'],
                             record['error'],record['mode'],record['work'])
        append_record(self.root/'response_receipts.jsonl',{k:record[k] for k in ('run','sent','received','status','error','body_sha256')})

    def start(self):
        if self.started_once:raise ValueError('Queued collector requires a fresh instance for every run.')
        self.started_once=True;self.writer=ResultWriter(self.persist,self.writer_error)
        try:
            run=super().start()
            with self.journal.db() as db:
                config=json.loads(db.execute('SELECT config FROM runs WHERE id=?',(run['id'],)).fetchone()[0])
                config['persistence']={'version':VERSION,'queue_capacity':CAPACITY,'http_workers':2,'single_writer':True}
                db.execute('UPDATE runs SET config=? WHERE id=?',(json.dumps(config),run['id']))
            return run
        except BaseException:
            self.stop_event.set()
            try:self.writer.finish()
            except Exception:pass
            raise

    def acquire_response(self,run):
        sent=time.time();clock=time.perf_counter();status=error=mode=work=fingerprint=None
        try:
            with open_direct(self.url+'/work',timeout=2) as response:
                status=response.status;mode=response.headers.get('X-Workbench-Mode');body=response.read()
                fingerprint=hashlib.sha256(body).hexdigest()
                work=dict(cpu_ms=float(response.headers['X-Workbench-Extra-CPU-Ms']),
                          wall_ms=float(response.headers['X-Workbench-Extra-Wall-Ms']),
                          iterations=int(response.headers['X-Workbench-Extra-Iterations']))
        except Exception as exc:error=type(exc).__name__
        return dict(run=run,sent=sent,received=time.time(),duration_ms=(time.perf_counter()-clock)*1000,
                    status=status,error=error,mode=mode,work=work,body_sha256=fingerprint)

    def one_request(self,run):
        # The inherited two-worker callback releases its slot after this enqueue,
        # not after SQLite/receipt I/O. Queue capacity is NEVER bypassed.
        self.writer.submit(self.acquire_response(run))

    def workload(self,run):
        try:super().workload(run)
        finally:
            try:self.writer.finish()
            except Exception as exc:self.writer_error(str(exc))

    def close(self):
        super().close()
        if self.writer is None:return
        error=None
        try:self.writer.finish()
        except Exception as exc:error=str(exc)
        status=self.writer.status()
        if self.current:
            path=self.root/'collector_closed.json';closed=json.loads(path.read_text(encoding='utf-8'))
            closed['persistence']=status
            closed['all_submitted_results_persisted']=status['complete'] and status['persisted']==self.submitted
            write_json(path,closed)
            write_json(self.root/'writer_status.json',status)
            partial=self.writer.incomplete_records()
            if partial and not (self.root/'incomplete_results.jsonl').exists():
                for record in partial:append_record(self.root/'incomplete_results.jsonl',record)
        if error or not status['complete'] or (self.current and status['persisted']!=self.submitted):
            raise RuntimeError('Collection cannot be sealed: '+str(status))


def collect(output,order):
    output=Path(output).resolve()
    result=base_collect(output,order,profile='cycles_v1',bench_factory=QueuedCycleBench)
    closed=json.loads((output/'testbench/collector_closed.json').read_text(encoding='utf-8'))
    if not closed.get('all_submitted_results_persisted') or not closed['persistence']['complete']:
        raise ValueError('Writer did not fully persist submitted results.')
    with closing(sqlite3.connect((output/'testbench/application.sqlite3').as_uri()+'?mode=ro',uri=True)) as original:
        with closing(sqlite3.connect(output/'application_snapshot.sqlite3')) as destination:
            original.backup(destination);destination.execute('PRAGMA journal_mode=DELETE')
    # Writer completion/counts are inside collector_closed.json, already required
    # and covered by the unchanged collection seal and downstream pipeline.
    seal_collection(output,result['run']);return result


def main():
    parser=argparse.ArgumentParser(description='Отдельный сборщик с ограниченной очередью; без --collect только расписание.')
    parser.add_argument('--collect',action='store_true');parser.add_argument('--order',type=int,choices=(1,2),default=1)
    parser.add_argument('--output',type=Path);args=parser.parse_args()
    if not args.collect:
        print(json.dumps(dict(preview(args.order),collector=VERSION,collection_started=False)));return
    if args.output is None:parser.error('--output required')
    collect(args.output,args.order)


if __name__=='__main__':main()
