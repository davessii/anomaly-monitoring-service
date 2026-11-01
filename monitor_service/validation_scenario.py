"""Fixed prospective scenario, using the existing main-service API.

Default: verify the bundle and print the schedule; no network or observations.
Only --execute starts one fresh real recording. It does NOT grade its quality.
"""
import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = Path(__file__).with_name('validation_v1')
RESULTS = ROOT / 'outputs/service-validation-v1'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, value):
    with path.open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)


def verify_bundle(bundle=BUNDLE):
    seal = json.loads((bundle / 'seal.json').read_text(encoding='utf-8'))
    if seal['id'] != 'service-validation-v1' or seal['version'] != '1.0.0':
        raise ValueError('Неизвестная фиксация сценария.')
    for name, expected in seal['files'].items():
        path = (ROOT / name).resolve()
        if not path.is_relative_to(ROOT) or not path.is_file() or sha(path) != expected:
            raise ValueError('Изменился закреплённый файл: ' + name)
    return sha(bundle / 'seal.json')


def load_protocol(bundle=BUNDLE):
    return json.loads((bundle / 'protocol.json').read_text(encoding='utf-8'))


def schedule(protocol, number):
    run = next(row for row in protocol['runs'] if row['run'] == number)
    result, offset = [], 0
    for name in protocol['generator']['stage_order']:
        seconds = run[name]
        result.append(dict(stage=name, start_seconds=offset,
                           duration_seconds=seconds, end_seconds=offset + seconds))
        offset += seconds
    return result


class Clock:
    def stamp(self):
        return dict(utc=time.time(), monotonic=time.monotonic())

    def monotonic(self):
        return time.monotonic()

    def sleep(self, seconds):
        time.sleep(seconds)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class Api:
    def __init__(self, url, timeout):
        parsed = urllib.parse.urlsplit(url)
        if (parsed.scheme != 'http' or parsed.hostname not in ('127.0.0.1', 'localhost')
                or parsed.username or parsed.password or parsed.path not in ('', '/')
                or parsed.query or parsed.fragment or not parsed.port):
            raise ValueError('Нужен локальный адрес сервиса, например http://127.0.0.1:8765.')
        self.url = url.rstrip('/')
        self.timeout = timeout
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def call(self, path, body=None):
        data = None if body is None else json.dumps(body, allow_nan=False).encode('utf-8')
        request = urllib.request.Request(self.url + path, data=data,
                                         headers={} if data is None else {'Content-Type': 'application/json'})
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                raw = response.read(16 * 1024 * 1024 + 1)
                if len(raw) > 16 * 1024 * 1024:
                    raise ValueError('Слишком большой ответ сервиса.')
                return json.loads(raw)
        except urllib.error.HTTPError as exc:
            message = exc.read(4096).decode('utf-8', errors='replace')
            exc.close()
            raise RuntimeError('Команда сервиса отклонена: ' + message) from exc


class Events:
    def __init__(self, path, clock):
        self.stream = path.open('x', encoding='utf-8')
        self.clock = clock

    def add(self, kind, **values):
        row = dict(kind=kind, **self.clock.stamp(), **values)
        self.stream.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + '\n')
        self.stream.flush()

    def close(self):
        self.stream.close()


class Evidence:
    """Read ONLY the selected run from already existing service databases."""
    def __init__(self, data_dir):
        self.data_dir = Path(data_dir).resolve()
        self.monitor = self.data_dir / 'monitor.sqlite3'
        self.application = self.data_dir / 'testbench/application.sqlite3'

    @staticmethod
    def connect(path):
        if not path.is_file():
            raise ValueError('Не найдена база работающего сервиса: ' + str(path))
        db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)
        db.row_factory = sqlite3.Row
        return db

    def model(self, host):
        with closing(self.connect(self.monitor)) as db:
            row = db.execute('SELECT state FROM hosts WHERE id=?', (host,)).fetchone()
            if row is None:
                raise ValueError('Выбранная папка данных не принадлежит этому запуску сервиса.')
            return json.loads(row['state'])

    def check_model(self, host, protocol):
        state = self.model(host)
        config = protocol['source_config']
        if (state['config'] != protocol['model_config'] or state['lengths'] != config['lengths']
                or state['features'] != config['features'] or state['log_settings'] != config['log_settings']
                or state['processing_mode'] != config['processing_mode'] or not state['ready']):
            raise ValueError('Сохранённая модель не соответствует фиксированным настройкам.')
        return state

    def runtime(self, run, protocol):
        with closing(self.connect(self.application)) as db:
            row = db.execute('SELECT config FROM runs WHERE id=? AND host=?', (run, run)).fetchone()
            if row is None:
                raise ValueError('Не найдена запись этого приложения в указанной папке данных.')
            config = json.loads(row['config'])
        actual = config['runtime']
        if any(actual[key] != value for key, value in protocol['runtime_versions'].items()):
            raise ValueError('Изменилась записанная среда Python/NumPy.')
        if config['access_log']['profile'] != protocol['source_config']['profile']:
            raise ValueError('Приложение пишет другой формат журнала.')
        return config

    def export(self, output, run, source, expected_name):
        if source:
            with closing(self.connect(self.monitor)) as db:
                row = db.execute('SELECT host,path FROM log_sources WHERE id=?', (source,)).fetchone()
                if row is None or row['host'] != source:
                    raise ValueError('Не совпала привязка журнала и модели.')
                path = Path(row['path']).resolve()
                if path.name != expected_name or expected_name != run + '-access.log':
                    raise ValueError('Не совпал файл именно этого учебного приложения.')
                with path.open('rb') as src, (output / 'access.log').open('xb') as dst:
                    shutil.copyfileobj(src, dst)
                self.rows(db, output / 'observations.jsonl', 'SELECT * FROM observations WHERE host=? ORDER BY ts', source)
                self.rows(db, output / 'messages.jsonl', 'SELECT * FROM log_messages WHERE host=? ORDER BY ts', source)
                self.rows(db, output / 'windows.jsonl', 'SELECT * FROM log_windows WHERE source=? ORDER BY start_ms', source)
            save(output / 'model_after.json', self.model(source))
        if run:
            with closing(self.connect(self.application)) as db:
                for table, ordering in (('requests', 'id'), ('controls', 'id'), ('load_changes', 'id')):
                    self.rows(db, output / (table + '.jsonl'),
                              'SELECT * FROM ' + table + ' WHERE run=? ORDER BY ' + ordering, run)

    @staticmethod
    def rows(db, path, query, key):
        with path.open('x', encoding='utf-8') as stream:
            for row in db.execute(query, (key,)):
                stream.write(json.dumps(dict(row), ensure_ascii=False, allow_nan=False) + '\n')


class Scenario:
    def __init__(self, protocol, number, api, evidence, output, *, clock=None, check_bundle=verify_bundle):
        self.protocol, self.number, self.api = protocol, number, api
        self.evidence, self.output = evidence, Path(output)
        self.clock = clock or Clock()
        self.check_bundle = check_bundle
        self.run = self.source = self.filename = None
        self.events = None
        self.commands = []
        self.phase = 'preflight'
        self.origin = None
        self.seal_hash = None
        self.preexisting_app_ids = set()

    def fixed_bundle(self):
        digest = self.check_bundle()
        if self.seal_hash is not None and digest != self.seal_hash:
            raise ValueError('Фиксация сценария была заменена после начала записи.')
        return digest

    def command(self, path, body):
        before = self.clock.stamp()
        self.events.add('command_sent', phase=self.phase, path=path, body=body)
        try:
            answer = self.api.call(path, body)
        except Exception:
            self.events.add('command_failed', phase=self.phase, path=path, body=body,
                            sent=before, side_effects_may_have_occurred=True)
            raise
        after = self.clock.stamp()
        item = dict(phase=self.phase, path=path, body=body, sent=before, acknowledged=after)
        self.commands.append(item)
        self.events.add('command_acknowledged', **item)
        return answer

    def preflight(self):
        health = self.api.call('/api/health')
        if health.get('version') != self.protocol['service_version'] or health.get('precise_log_quality') is not True:
            raise ValueError('Нужен закреплённый основной сервис версии 0.7.0.')
        self.evidence.connect(self.evidence.monitor).close()
        self.evidence.connect(self.evidence.application).close()
        bench = self.api.call('/api/bench')
        logs = self.api.call('/api/logs')
        state = self.api.call('/api/state')
        self.preexisting_app_ids = {row['id'] for row in bench['runs']}
        if (bench.get('current', {}) and bench['current'].get('running')
                or any(s['worker']['running'] for s in logs['sources'])
                or any(job.get('running') for job in state['jobs'])):
            raise ValueError('Перед записью остановите другие сборщики и учебное приложение. Генератор их не останавливает.')

    def health(self):
        if self.origin is not None:
            stamp = self.clock.stamp()
            drift = (stamp['utc'] - self.origin['utc']) - (stamp['monotonic'] - self.origin['monotonic'])
            if abs(drift) > self.protocol['generator']['max_clock_drift_seconds']:
                raise RuntimeError('Обычные часы изменились относительно монотонных больше чем на секунду.')
        bench = self.api.call('/api/bench')
        current = bench.get('current')
        if not current or current['id'] != self.run or not current['running'] or current.get('error'):
            raise RuntimeError('Учебное приложение остановилось или было заменено.')
        logs = self.api.call('/api/logs')
        source = next((s for s in logs['sources'] if s['source'] == self.source), None)
        if (not source or source['status'] != 'active' or not source['worker']['running']
                or source['worker'].get('error') or source['model']['phase'] == 'unfit'):
            raise RuntimeError('Чтение или начальная настройка непригодны; попытка не заменяется.')
        if source['config'] != self.protocol['source_config']:
            raise ValueError('Изменились настройки источника.')
        app = next(row for row in bench['runs'] if row['id'] == self.run)
        # No decision flags or quality counts are inspected here.
        ready = source['model']['ready'] and app['monitor']['phase'] == 'monitoring'
        self.events.add('operational_health', phase=self.phase, ready=bool(ready),
                        log_samples=source['model']['samples'], app_samples=app['monitor']['samples'])
        return ready

    def warmup(self):
        self.phase = 'warmup'
        start = self.clock.monotonic()
        settings = self.protocol['source_config']['log_settings']
        app = self.command('/api/bench/start', {})
        interval = self.commands[-1]
        if (app['id'] in self.preexisting_app_ids or not app.get('running')
                or type(app.get('started_at')) not in (int, float)
                or not interval['sent']['utc'] <= app['started_at'] <= interval['acknowledged']['utc']):
            raise ValueError('Сервис не подтвердил создание нового приложения этой командой; чужой запуск не используется и не останавливается.')
        self.run, self.filename = app['id'], app['access_log_file']
        if app.get('access_log_profile') != self.protocol['source_config']['profile']:
            raise ValueError('Новый запуск не использует точные измерения.')
        connected = self.command('/api/logs/start', dict(filename=self.filename,
            profile=self.protocol['source_config']['profile'], handler_limit_ms=settings['handler_limit_ms'],
            allowed_error_fraction=settings['allowed_error_fraction']))
        self.source = connected['id']
        if connected['host'] != self.source or self.source == self.run:
            raise ValueError('Нужна отдельная новая модель журнала.')
        save(self.output / 'runtime.json', self.evidence.runtime(self.run, self.protocol))
        limit = self.protocol['generator']['warmup_timeout_seconds']
        while self.clock.monotonic() - start < limit:
            if self.health():
                save(self.output / 'model_before.json', self.evidence.check_model(self.source, self.protocol))
                self.events.add('warmup_complete', elapsed_seconds=self.clock.monotonic() - start)
                return
            self.clock.sleep(self.protocol['generator']['poll_seconds'])
        raise TimeoutError('Начальная настройка не завершилась за 1200 секунд.')

    def apply_phase(self, item):
        self.phase = item['stage']
        seconds = item['duration_seconds']
        if self.phase == 'load':
            return self.command('/api/bench/load', dict(rate=self.protocol['generator']['high_rps'], seconds=seconds))
        if self.phase == 'delay':
            self.command('/api/bench/load', dict(rate=self.protocol['generator']['normal_rps'], seconds=seconds))
        mode = {'normal': 'normal', 'delay': 'delay', 'errors': 'error', 'recovery': 'normal'}[self.phase]
        return self.command('/api/bench/control', dict(mode=mode, seconds=seconds))

    def wait(self, deadline):
        next_probe = self.clock.monotonic()
        while self.clock.monotonic() < deadline:
            if self.clock.monotonic() >= next_probe:
                if not self.health():
                    raise RuntimeError('Готовность модели потеряна после начала сценария.')
                next_probe = self.clock.monotonic() + self.protocol['generator']['poll_seconds']
            remaining = deadline - self.clock.monotonic()
            if remaining > 0:
                self.clock.sleep(min(.25, remaining))

    def play(self):
        origin = self.clock.stamp()
        self.origin = origin
        self.events.add('scenario_begin', origin=origin, schedule=schedule(self.protocol, self.number))
        boundaries = []
        for item in schedule(self.protocol, self.number):
            self.fixed_bundle()
            self.evidence.check_model(self.source, self.protocol)
            self.events.add('phase_begin', **item)
            self.apply_phase(item)
            late = self.clock.monotonic() - (origin['monotonic'] + item['start_seconds'])
            self.events.add('phase_applied', phase=item['stage'], boundary_lateness_seconds=late)
            boundaries.append(dict(**item, applied=self.clock.stamp(), boundary_lateness_seconds=late))
            if late > self.protocol['generator']['max_boundary_lateness_seconds']:
                raise RuntimeError('Граница этапа опоздала больше чем на одну секунду; запись непригодна.')
            self.wait(origin['monotonic'] + item['end_seconds'])
        self.events.add('scenario_end', elapsed_seconds=self.clock.monotonic() - origin['monotonic'])
        self.phase = 'delivery_drain'
        self.wait(self.clock.monotonic() + self.protocol['generator']['delivery_drain_seconds'])
        return dict(origin=origin, boundaries=boundaries)

    def cleanup(self):
        """Stop only the source/app whose successful creation was acknowledged."""
        self.phase = 'cleanup'
        issues = []
        for path, body in (( '/api/logs/stop', {'source_id': self.source}) if self.source else (None, None),
                           ( '/api/bench/stop', {}) if self.run else (None, None)):
            if path is None:
                continue
            try:
                if path == '/api/bench/stop':
                    current = self.api.call('/api/bench').get('current')
                    if not current or current['id'] != self.run:
                        raise RuntimeError('Другой запуск приложения не останавливается.')
                self.command(path, body)
                until = self.clock.monotonic() + self.protocol['generator']['cleanup_timeout_seconds']
                while True:
                    if path == '/api/logs/stop':
                        sources = self.api.call('/api/logs')['sources']
                        active = any(s['source'] == self.source and s['worker']['running'] for s in sources)
                    else:
                        current = self.api.call('/api/bench').get('current')
                        active = bool(current and current['id'] == self.run and current['running'])
                    if not active:
                        break
                    if self.clock.monotonic() >= until:
                        raise TimeoutError('Не подтверждена остановка принадлежащего записи процесса.')
                    self.clock.sleep(.2)
            except Exception as exc:
                issues.append(str(exc))
        return issues

    def execute(self):
        seal_hash = self.fixed_bundle()
        self.seal_hash = seal_hash
        self.output.mkdir(parents=True, exist_ok=False)  # Never replace a failed attempt.
        receipt = dict(protocol=self.protocol['id'], protocol_seal_sha256=seal_hash, run_number=self.number,
                       status='invalid', schedule=schedule(self.protocol, self.number),
                       quality_evaluated=False, started=self.clock.stamp())
        save(self.output / 'attempt.json', receipt)
        self.events = Events(self.output / 'scenario_events.jsonl', self.clock)
        try:
            self.preflight()
            self.warmup()
            receipt.update(self.play(), status='recorded_not_evaluated')
            self.fixed_bundle()
        except (Exception, KeyboardInterrupt) as exc:
            receipt.update(status='invalid', error=type(exc).__name__ + ': ' + str(exc))
            self.events.add('recording_interrupted', error=receipt['error'])
        finally:
            issues = self.cleanup()
            try:
                self.evidence.export(self.output, self.run, self.source, self.filename)
            except Exception as exc:
                issues.append('Сохранение исходных данных: ' + str(exc))
            if issues:
                receipt.update(status='invalid', cleanup_or_export_errors=issues)
            receipt.update(application=self.run, source=self.source, commands=self.commands,
                           finished=self.clock.stamp(), file_sha256={p.name: sha(p) for p in self.output.iterdir()
                           if p.is_file() and p.name != 'scenario_events.jsonl'})
            self.events.add('recording_closed', status=receipt['status'], quality_evaluated=False)
            self.events.close()
            receipt['file_sha256']['scenario_events.jsonl'] = sha(self.output / 'scenario_events.jsonl')
            save(self.output / 'run_manifest.json', receipt)
        return receipt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true', help='Begin one real fresh recording; otherwise print only')
    parser.add_argument('--run', type=int, choices=range(1, 6))
    parser.add_argument('--service-url', default='http://127.0.0.1:8765')
    parser.add_argument('--data-dir', type=Path, default=Path(__file__).with_name('data'))
    args = parser.parse_args(argv)
    if args.execute and args.run is None:
        parser.error('--execute требует явного --run от 1 до 5.')
    seal_hash = verify_bundle()
    protocol = load_protocol()
    if not args.execute:
        print(json.dumps(dict(action='describe_only', real_recordings_started=0,
            protocol_seal_sha256=seal_hash,
            schedules={str(n): schedule(protocol, n) for n in ([args.run] if args.run else range(1, 6))}),
            ensure_ascii=False, indent=2))
        return 0
    runner = Scenario(protocol, args.run, Api(args.service_url, protocol['generator']['http_timeout_seconds']),
                      Evidence(args.data_dir), RESULTS / f'run-{args.run:02d}')
    result = runner.execute()
    print(json.dumps(dict(status=result['status'], quality_evaluated=False,
                         folder=str(runner.output)), ensure_ascii=False, indent=2))
    return 0 if result['status'] == 'recorded_not_evaluated' else 1


if __name__ == '__main__':
    raise SystemExit(main())
