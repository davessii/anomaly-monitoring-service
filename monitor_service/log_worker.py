"""Backend-owned log workers; filenames only, under one configured directory."""
from pathlib import Path
import json
import logging
import stat
import threading
import time
import uuid

from .store import Conflict
from .log_ingest import LogIngestor, LogIngestError
from .log_schema import FEATURES
from .log_tailer import POLL_INTERVAL_MS
from .immediate import MODE
from .log_precise import PROFILE as PRECISE_PROFILE
from .log_schema import PROFILE
from .log_detector import MODE as LOG_MODE

MAX_WORKERS = 4
DELIVERIES_PER_TICK = 32
REASONS = {
    'file_missing': 'Файл исчез. Нужна проверка источника и новое наблюдение.',
    'file_replaced': 'Файл заменён. Продолжать прежнюю модель нельзя.',
    'file_shrank': 'Файл обрезан. Продолжать прежнюю модель нельзя.',
    'file_unreadable': 'Не удалось прочитать файл. Проверьте доступ к нему.',
    'file_content_changed': 'Уже прочитанная часть файла изменилась.',
    'ambiguous_file_change': 'Файл изменён неподдерживаемым способом.',
    'clock_went_backwards': 'Время на компьютере пошло назад. Чтение остановлено.',
    'delivery_clock_went_backwards': 'Нарушен порядок времени. Доставка остановлена.',
    'event_is_in_the_future': 'Время запроса в журнале опережает часы сервиса.',
    'checkpoint_or_configuration_mismatch': 'Не удалось проверить сохранённое состояние или настройки.',
    'window_hash_mismatch': 'Содержимое сохранённого интервала не совпало с проверочной суммой.',
    'model_changed_outside_log_delivery': 'Состояние модели изменилось вне чтения этого журнала.',
    'data_quality': 'Есть пустые, повреждённые или поздние данные. Подача новых показателей в модель приостановлена.',
    'source_needs_restart': 'Источник заблокирован. Проверьте причину и создайте новое наблюдение.',
    'file_access_restricted': 'Файл находится вне папки журналов или заменён ссылкой. Чтение остановлено.',
}


def reason_text(reason):
    return REASONS.get(reason, 'Чтение остановилось. Подробности записаны в технический журнал сервиса.' if reason else None)


class LogWorkers:
    def __init__(self, store, directory, *, clock_ms=None):
        self.store = store
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.ingest = LogIngestor(store)
        self.clock_ms = clock_ms or (lambda: time.time_ns()//1_000_000)
        self.lock = threading.RLock()
        self.jobs = {}
        self.closing = False

    def _file(self, filename):
        if (type(filename) is not str or not 1 <= len(filename) <= 255 or
                filename in ('.', '..') or '/' in filename or '\\' in filename or ':' in filename or
                Path(filename).name != filename or Path(filename).suffix.lower() != '.log'):
            raise ValueError('Выберите файл .log из списка доступных журналов.')
        candidate = self.directory/filename
        try:
            info = candidate.lstat()
            if (candidate.is_symlink() or getattr(info, 'st_file_attributes', 0) & 0x400 or
                    not stat.S_ISREG(info.st_mode) or candidate.resolve().parent != self.directory):
                raise ValueError('Доступен только обычный файл внутри папки журналов, без ссылок.')
        except OSError as exc:
            raise ValueError('Файл журнала не найден или недоступен.') from exc
        return candidate

    def files(self):
        result = []
        for candidate in sorted(self.directory.iterdir()):
            if len(result) >= 256:
                break
            if candidate.suffix.lower() != '.log':
                continue
            try:
                safe = self._file(candidate.name)
                with safe.open('rb') as stream:prefix=stream.read(3)
                result.append(dict(name=safe.name, size=safe.stat().st_size,
                                   profile=PRECISE_PROFILE if prefix==b'v2 ' else PROFILE))
            except ValueError:
                continue
        return result

    def _record(self, source_id):
        with self.store.connection() as db:
            row = db.execute('SELECT * FROM log_sources WHERE id=?', (source_id,)).fetchone()
        if row is None:
            raise KeyError('Наблюдение за журналом не найдено.')
        return dict(row)

    def _source_file(self, record):
        path = Path(record['path'])
        if path.parent != self.directory:
            raise ValueError('Сохранённый источник находится вне настроенной папки журналов.')
        safe = self._file(path.name)
        if safe != path:
            raise ValueError('Путь сохранённого источника не совпал с выбранным файлом.')
        return safe

    def start(self, data):
        if not isinstance(data, dict) or set(data) not in ({'filename'}, {'source_id'}, {'filename','profile','handler_limit_ms'}, {'filename','profile','handler_limit_ms','allowed_error_fraction'}):
            raise ValueError('Укажите один файл для нового наблюдения или одно сохранённое наблюдение.')
        with self.lock:
            if self.closing:
                raise Conflict('Сервис останавливается.')
            running = [job for job in self.jobs.values() if job['thread'].is_alive()]
            if 'source_id' in data:
                source_id = data['source_id']
                if type(source_id) is not str:
                    raise ValueError('Выберите сохранённое наблюдение.')
                record = self._record(source_id)
                path = self._source_file(record)
                existing = self.jobs.get(source_id)
                if existing and existing['thread'].is_alive():
                    return dict(id=source_id, host=record['host'], already_running=True,
                                stopping=existing['stop'].is_set())
                status = self.ingest.status(source_id)
                if status['status'] == 'needs_restart':
                    raise Conflict(reason_text(status['reason']))
            else:
                path = self._file(data['filename'])
                with path.open('rb') as stream:prefix=stream.read(3)
                profile=data.get('profile',PRECISE_PROFILE if prefix==b'v2 ' else PROFILE)
                if profile not in (PROFILE,PRECISE_PROFILE):raise ValueError('Неизвестный формат журнала.')
                if prefix and (prefix==b'v2 ')!=(profile==PRECISE_PROFILE):raise ValueError('Выбранный формат не совпал со строками файла.')
                options=None
                if profile==PRECISE_PROFILE:
                    if 'handler_limit_ms' not in data:raise ValueError('Задайте допустимое время обработки для нового журнала.')
                    options=dict(handler_limit_ms=data['handler_limit_ms'],allowed_error_fraction=data.get('allowed_error_fraction',0.))
                    from .log_ingest import configuration
                    configuration(profile,options)  # Validate BEFORE registering an unused host.
                elif 'handler_limit_ms' in data:raise ValueError('Настройки качества требуют точного формата v2.')
                for job in running:
                    if job['path'] == path:
                        raise Conflict('Этот файл уже читается. Выберите его сохранённое наблюдение.')
                source_id = None
            if len(running) >= MAX_WORKERS:
                raise Conflict('Одновременно можно читать до четырёх журналов. Остановите один из них.')
            if any(job['path'] == path for job in running):
                raise Conflict('Этот файл уже читается в другом наблюдении.')
            if source_id is None:
                source_id = 'log-'+uuid.uuid4().hex[:12]
                self.store.register(dict(id=source_id, name='Журнал · '+path.name[:145], source='external',
                                         features=list(FEATURES), settings=dict(components=1, processing_mode=LOG_MODE if profile==PRECISE_PROFILE else MODE,**(options or {}))))
                try:
                    now = self.clock_ms()
                    self.ingest.attach(source_id, source_id, path, start_ms=(now//1000+1)*1000,profile=profile,settings=options)
                except Exception:
                    # Remove ONLY this fresh, unbound, never-observed model on attach failure.
                    with self.store.lock, self.store.connection() as db:
                        db.execute('BEGIN IMMEDIATE')
                        db.execute('DELETE FROM hosts WHERE id=? AND last_ts IS NULL '
                                   'AND NOT EXISTS(SELECT 1 FROM observations WHERE host=?) '
                                   'AND NOT EXISTS(SELECT 1 FROM log_sources WHERE host=?)',
                                   (source_id, source_id, source_id))
                    raise
                record = self._record(source_id)
            job = dict(path=path, stop=threading.Event(), error=None, last_poll_ms=None,
                       backpressure=False)
            thread = threading.Thread(target=self._run, args=(source_id, job),
                                      daemon=True, name='access-log-'+source_id)
            job['thread'] = thread
            self.jobs[source_id] = job
            thread.start()
            return dict(id=source_id, host=record['host'], already_running=False)

    def tick(self, source_id, *, stop=None):
        """One bounded worker tick; explicit method also supports software tests."""
        record = self._record(source_id)
        try:
            self._source_file(record)  # Recheck the allowlist before every file read.
        except ValueError as exc:
            code = 'file_missing' if not Path(record['path']).exists() else 'file_access_restricted'
            with self.store.lock, self.store.connection() as db:
                db.execute('BEGIN IMMEDIATE')
                self.ingest._block(db, source_id, self.clock_ms(), code)
            raise LogIngestError(code) from exc
        if stop is not None and stop.is_set():
            return dict(stopped=True)
        result = self.ingest.poll(source_id, now_ms=self.clock_ms(), read_clock_ms=self.clock_ms)
        for _ in range(DELIVERIES_PER_TICK):
            if stop is not None and stop.is_set():
                return dict(stopped=True)
            delivered = self.ingest.deliver(source_id, now_ms=self.clock_ms())
            if delivered['status'] == 'nothing_ready':
                break
        return result

    def _run(self, source_id, job):
        try:
            while not job['stop'].is_set():
                result = self.tick(source_id, stop=job['stop'])
                with self.lock:
                    job['last_poll_ms'] = self.clock_ms()
                    job['backpressure'] = result.get('status') == 'backpressure'
                if job['stop'].wait(POLL_INTERVAL_MS/1000):
                    break
        except Exception as exc:
            with self.lock:
                job['error'] = reason_text(exc.code) if isinstance(exc, LogIngestError) else str(exc) if isinstance(exc, ValueError) else reason_text('worker_failure')
            logging.exception('Access log worker stopped: %s', source_id)

    def stop(self, source_id):
        record = self._record(source_id)
        with self.lock:
            job = self.jobs.get(source_id)
            if job:
                job['stop'].set()
        if job:
            job['thread'].join(timeout=2)
        return dict(id=source_id, host=record['host'], running=bool(job and job['thread'].is_alive()))

    def close(self):
        with self.lock:
            self.closing = True
            jobs = list(self.jobs.values())
            for job in jobs:
                job['stop'].set()
        deadline = time.monotonic()+3
        for job in jobs:
            job['thread'].join(timeout=max(0, deadline-time.monotonic()))

    def sources(self):
        with self.store.connection() as db:
            records = db.execute('SELECT id,host,path,created,config FROM log_sources ORDER BY created').fetchall()
            hosts = {row['id']: json_state(row) for row in db.execute('SELECT id,state FROM hosts')}
        result = []
        for record in records:
            status = self.ingest.status(record['id'])
            with self.lock:
                job = self.jobs.get(record['id'])
                live = dict(running=bool(job and job['thread'].is_alive()),
                            stopping=bool(job and job['stop'].is_set() and job['thread'].is_alive()),
                            error=job['error'] if job else None,
                            last_poll_ms=job['last_poll_ms'] if job else None,
                            backpressure=job['backpressure'] if job else False)
            with self.store.connection() as db:
                latest = db.execute('SELECT payload,status FROM log_windows WHERE source=? ORDER BY start_ms DESC LIMIT 1', (record['id'],)).fetchone()
            status.update(filename=Path(record['path']).name, created=record['created'], worker=live,
                          reason_text=reason_text(status['reason']), model=hosts.get(record['host']),
                          latest_window=dict(payload=json.loads(latest['payload']), delivery=latest['status']) if latest else None)
            status['config']=json.loads(record['config'])
            status['latest_message']=next(iter(self.store.log_messages(record['host'],1)),None)
            status['messages']=self.store.log_messages(record['host'],30,events_only=True)
            result.append(status)
        return result

    def state(self):
        return dict(directory=str(self.directory), files=self.files(), sources=self.sources(),
                    initial_observations=896, window_seconds=1, waiting_seconds=2)


def json_state(row):
    state = json.loads(row['state'])
    return dict(samples=state['seen'], required=sum(state['lengths']), ready=state['ready'],
                phase=state.get('phase','monitoring' if state['ready'] else 'warmup'),
                log_settings=state.get('log_settings'),update_counts=state.get('candidate',{}).get('update_counts'))
