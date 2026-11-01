"""Explicit standalone collection launcher; default only prints the schedule.

Never opens existing user databases, calls a remote server or evaluates quality.
Use --collect only after the technical checks and a separate decision to collect.
"""
import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import time
import zipfile
from .efficiency_schedule import Schedule, preview


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, allow_nan=False, indent=2), encoding='utf-8')
    temporary.replace(path)


def append_record(path, value):
    with path.open('a', encoding='utf-8') as stream:
        stream.write(json.dumps(value, ensure_ascii=False, allow_nan=False) + '\n')


def code_snapshot(output):
    package = Path(__file__).resolve().parent
    plan = package / 'План короткой проверки расхода ресурсов — v0.1.md'
    files = sorted(package.glob('*.py')) + [plan,package/'Изменение плана измерений — v0.2.md']
    records = []
    with zipfile.ZipFile(output / 'code.zip', 'x', zipfile.ZIP_DEFLATED) as archive:
        for path in files:
            body = path.read_bytes()
            archive.writestr(path.name, body)
            records.append({'path': path.name, 'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()})
    write_json(output / 'source_manifest.json', {'files': records, 'external_libraries_in_archive': False,
                                               'scope': 'Development snapshot, not a confirmation freeze.'})


def perform(bench, action):
    if action.kind == 'load':
        return bench.set_load(action.value, action.seconds)
    if action.kind == 'normal':
        return bench.control('normal', action.seconds)
    if action.kind == 'extra_cpu':
        return bench.control('extra_cpu', action.seconds)
    if action.kind == 'stop':
        return bench.stop()
    raise ValueError('Unknown schedule action.')


def collect(output, order, *, clock=time.monotonic, sleep=time.sleep, profile='cpu_percent_v1', bench_factory=None):
    """One owned bench in a new directory. No automatic repeat/resume/quality."""
    schedule_config = preview(order)  # Validate before imports or any filesystem mutation.
    if profile not in ('cpu_percent_v1','cycles_v1'):raise ValueError('Unknown input profile.')
    schedule_config['input_profile']=profile
    # These are environment settings, not a claim of runtime BLAS verification.
    for key in ('OPENBLAS_NUM_THREADS', 'OPENBLAS_DEFAULT_NUM_THREADS', 'OMP_NUM_THREADS', 'MKL_NUM_THREADS'):
        os.environ[key] = '1'
    # Lazy imports: preview does not import NumPy or initialize any database.
    from .store import Store
    from .bench import Bench
    if profile=='cycles_v1':
        from .cycle_profile import CycleBench
        bench_type=CycleBench
    else:bench_type=Bench
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    code_snapshot(output)
    write_json(output / 'schedule.json', schedule_config)
    status = {'status': 'starting', 'order': order, 'scientific_confirmation': False,
              'quality_evaluated': False, 'input_profile':profile,'started_utc': datetime.now(timezone.utc).isoformat(),
              'scope': 'Isolated development collection; no dashboard server started.',
              'thread_environment_set_to_one': True, 'active_blas_threads_verified': False}
    write_json(output / 'collection_status.json', status)
    bench = None
    caught = None
    try:
        bench = (bench_factory or bench_type)(Store(output / 'monitor.sqlite3'))
        run = bench.start()
        status.update(status='collecting', run=run['id'])
        write_json(output / 'collection_status.json', status)
        scheduler = Schedule(order)
        started = clock()
        last_seen = -1
        last_progress = started
        while not scheduler.finished:
            with bench.lock:
                current = dict(bench.current)
            if not current['running'] or current.get('stopping') or bench.stop_event.is_set():
                raise RuntimeError('Collection ended before the complete schedule: ' + str(current.get('error')))
            # State is used exclusively for sample count/readiness, never quality.
            state = bench.store.state(run['host'])[0]
            now = clock()
            if last_seen < 0 and state['samples'] > 1:
                raise RuntimeError('Initial training milestone was missed.')
            if state['samples'] != last_seen:
                last_progress = now
                last_seen = state['samples']
                write_json(output / 'progress.json', {'samples': last_seen, 'required': 896,
                           'phase': state['phase'], 'post_elapsed_seconds': None if scheduler.post_origin is None else now - scheduler.post_origin,
                           'quality_evaluated': False})
            if now - last_progress > 10:
                raise RuntimeError('No sample progress for ten seconds.')
            if now - started > 1780:
                raise RuntimeError('Pilot time limit reached before schedule completion.')
            for action in scheduler.tick(state['samples'], state['phase'], now):
                wall = time.time()
                response = perform(bench, action)
                append_record(output / 'schedule_log.jsonl', {'action': asdict(action), 'samples': state['samples'],
                              'requested_wall_time': wall, 'completed_wall_time': time.time(),
                              'post_elapsed_seconds': None if scheduler.post_origin is None else now - scheduler.post_origin,
                              'result': response})
            sleep(.1)
        status['status'] = 'collection_complete_awaiting_measurement_audit'
    except BaseException as exc:
        caught = exc
        status.update(status='interrupted' if isinstance(exc, KeyboardInterrupt) else 'failed',
                      error=type(exc).__name__ + ': ' + str(exc))
    finally:
        if bench is not None:
            try:
                bench.stop()
                bench.close()
                if bench.thread and bench.thread.is_alive():
                    status.update(status='failed', error='Collection worker did not finish; record is incomplete.')
                elif bench.current and bench.current.get('error'):
                    status.update(status='failed', error=bench.current['error'])
            except BaseException as exc:
                status.update(status='failed', cleanup_error=type(exc).__name__ + ': ' + str(exc))
        status['ended_utc'] = datetime.now(timezone.utc).isoformat()
        status['quality_evaluated'] = False
        write_json(output / 'collection_status.json', status)
    if caught is not None:
        raise caught
    if status['status'] != 'collection_complete_awaiting_measurement_audit':
        raise RuntimeError(status.get('error', 'Incomplete collection.'))
    return status


def main(argv=None):
    parser = argparse.ArgumentParser(description='Расписание короткой проверки; без --collect ничего не запускается.')
    parser.add_argument('--order', type=int, choices=(1, 2), default=1)
    parser.add_argument('--collect', action='store_true', help='Начать ОДИН новый сбор, без оценки качества.')
    parser.add_argument('--profile',choices=('cpu_percent_v1','cycles_v1'),default='cpu_percent_v1')
    parser.add_argument('--output', type=Path, help='Только новая отдельная папка; существующая не перезаписывается.')
    args = parser.parse_args(argv)
    if args.collect and args.output is None:
        parser.error('--collect requires --output pointing to a new directory.')
    if not args.collect:
        print(json.dumps(dict(preview(args.order),input_profile=args.profile), ensure_ascii=False, indent=2))
        return
    result = collect(args.output, args.order,profile=args.profile)
    print('Сбор завершён. Сначала нужна проверка измерений; качество ещё не оценивалось.')
    print(result['run'])


if __name__ == '__main__':
    main()
