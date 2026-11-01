"""Explicit future collection with response fingerprints and a closed DB snapshot.

Import/preview never starts collection. Existing dashboard and old runs unchanged.
"""
import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from .bench import open_direct
from .cycle_profile import CycleBench, PROFILE
from .efficiency_pilot import collect as base_collect, append_record, write_json
from .efficiency_schedule import preview
from .cycle_outputs import sha


class RecordedCycleBench(CycleBench):
    def one_request(self, run):
        sent = time.time(); clock = time.perf_counter()
        status = error = mode = work = fingerprint = None
        try:
            with open_direct(self.url+'/work', timeout=2) as response:
                status = response.status; mode = response.headers.get('X-Workbench-Mode')
                body = response.read()
                fingerprint = hashlib.sha256(body).hexdigest()
                work = {'cpu_ms':float(response.headers['X-Workbench-Extra-CPU-Ms']),
                        'wall_ms':float(response.headers['X-Workbench-Extra-Wall-Ms']),
                        'iterations':int(response.headers['X-Workbench-Extra-Iterations'])}
        except Exception as exc:
            error = type(exc).__name__
        received = time.time(); duration = (time.perf_counter()-clock)*1000
        self.journal.request(run,sent,received,duration,status,error,mode,work)
        with self.lock:
            append_record(self.root/'response_receipts.jsonl',dict(run=run,sent=sent,received=received,
                          status=status,error=error,body_sha256=fingerprint))

    def close(self):
        super().close()
        if self.current:
            write_json(self.root/'collector_closed.json',dict(run=self.current['id'],
                own_child_exited=self.process is None or self.process.poll() is not None,
                collector_exited=self.thread is None or not self.thread.is_alive(),
                scheduled=self.scheduled,submitted=self.submitted,skipped=self.skipped))


def collect(output,order):
    output = Path(output).resolve()
    result = base_collect(output,order,profile=PROFILE,bench_factory=RecordedCycleBench)
    # Backup of a read-only source is a stable snapshot including committed WAL.
    source = output/'testbench/application.sqlite3'
    with closing(sqlite3.connect(source.as_uri()+'?mode=ro',uri=True)) as original:
        with closing(sqlite3.connect(output/'application_snapshot.sqlite3')) as destination:
            original.backup(destination)
            destination.execute('PRAGMA journal_mode=DELETE')
    seal_collection(output,result['run'])
    return result


def seal_collection(output,run):
    """Seal only a closed complete collection; used by deterministic fixtures too."""
    output=Path(output).resolve()
    snapshot=output/'application_snapshot.sqlite3'
    with snapshot.open('rb') as stream:header=stream.read(100)
    if header[:16]!=b'SQLite format 3\x00' or header[18:20]!=b'\x01\x01' or any(Path(str(snapshot)+suffix).exists() for suffix in ('-wal','-shm','-journal')):
        raise ValueError('Snapshot must be a closed rollback-mode database without sidecars.')
    status=json.loads((output/'collection_status.json').read_text(encoding='utf-8'))
    closed=json.loads((output/'testbench/collector_closed.json').read_text(encoding='utf-8'))
    if (status.get('status')!='collection_complete_awaiting_measurement_audit' or status.get('input_profile')!=PROFILE
        or status.get('run')!=run or closed.get('run')!=run or not closed.get('own_child_exited') or not closed.get('collector_exited')):
        raise ValueError('Only a closed complete cycles collection can be sealed.')
    paths=['application_snapshot.sqlite3','collection_status.json','schedule.json','schedule_log.jsonl',
           'testbench/response_receipts.jsonl','testbench/collector_closed.json',f'testbench/{run}-cycle-anchor.json',
           'source_manifest.json','code.zip']
    records=[dict(path=p,bytes=(output/p).stat().st_size,sha256=sha(output/p)) for p in paths]
    with (output/'collection_complete.json').open('x',encoding='utf-8') as stream:
        json.dump(dict(profile=PROFILE,run=run,files=records,quality_evaluated=False),stream,allow_nan=False)


def main():
    parser=argparse.ArgumentParser(description='Будущий сбор с проверяемыми ответами. Без --collect только расписание.')
    parser.add_argument('--order',type=int,choices=(1,2),default=1)
    parser.add_argument('--collect',action='store_true');parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    if not args.collect:
        print(json.dumps(dict(preview(args.order),profile=PROFILE,collection_started=False),ensure_ascii=False));return
    if args.output is None:parser.error('--output required')
    collect(args.output,args.order)
    print('Запись закрыта. Измерения и качество ещё не проверены.')


if __name__=='__main__':main()
