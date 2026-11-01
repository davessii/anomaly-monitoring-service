"""Durable log batches and atomic, idempotent window-to-detector delivery.

One caller-driven poll/deliver at a time, serialized by SQLite BEGIN IMMEDIATE.
No HTTP endpoints, background worker, automatic restart or experiment runner.
"""
import hashlib
import json
import time

from .store import Conflict, utc
from .immediate import MODE
from .log_schema import (FEATURES, PROFILE, METRICS_SCHEMA, WINDOW_MS, LATENESS_MS,
                         MAX_TIME_MS, MAX_EMIT_WINDOWS, checked_int)
from .log_tailer import LogTailer, TailerError, _identifier
from .log_parser import parse_line, LogParseError
from .log_aggregation import LogAggregator, AggregationError
from .log_checkpoint import VERSION, pack, unpack, window_from_dict
from .log_precise import (PROFILE as PRECISE_PROFILE,METRICS_SCHEMA as PRECISE_SCHEMA,
                          PreciseLogAggregator,parse_precise_line)
from .log_detector import MODE as LOG_MODE

MAX_READY_WINDOWS = 2048
MAX_DIAGNOSTICS = 500


def canonical(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(',', ':'))


def digest(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def configuration(profile=PROFILE,settings=None):
    if profile==PRECISE_PROFILE:
        from .log_detector import LogDetector
        settings=dict(settings or {})
        check=LogDetector(FEATURES,**settings)
        return dict(version=2,profile=PRECISE_PROFILE,schema=PRECISE_SCHEMA,
                    features=list(FEATURES),window_ms=WINDOW_MS,lateness_ms=LATENESS_MS,
                    components=1,processing_mode=LOG_MODE,lengths=[256,128,512],
                    log_settings=check.log_settings)
    if profile!=PROFILE:raise ValueError('Unknown log profile')
    return dict(version=VERSION, profile=PROFILE, schema=METRICS_SCHEMA,
                features=list(FEATURES), window_ms=WINDOW_MS, lateness_ms=LATENESS_MS,
                components=1, processing_mode=MODE, lengths=[256, 128, 512])


class LogIngestError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


class LogIngestor:
    def __init__(self, store, *, fault_hook=None):
        self.store = store
        self._fault_hook = fault_hook
        with store.lock, store.connection() as db:
            db.executescript('''
              CREATE TABLE IF NOT EXISTS log_sources(
                id TEXT PRIMARY KEY, host TEXT NOT NULL UNIQUE, path TEXT NOT NULL,
                generation TEXT NOT NULL, config TEXT NOT NULL, config_sha256 TEXT NOT NULL,
                host_state_sha256 TEXT NOT NULL, status TEXT NOT NULL, reason TEXT,
                last_delivery_ms INTEGER, created TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS log_checkpoints(
                source TEXT PRIMARY KEY, state TEXT NOT NULL, sha256 TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS log_windows(
                source TEXT NOT NULL, generation TEXT NOT NULL, start_ms INTEGER NOT NULL,
                payload TEXT NOT NULL, sha256 TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('ready','delivered','skipped')),
                receipt TEXT, receipt_sha256 TEXT, PRIMARY KEY(source,generation,start_ms));
              CREATE INDEX IF NOT EXISTS log_ready ON log_windows(source,status,start_ms);
              CREATE TABLE IF NOT EXISTS log_ingest_errors(
                id INTEGER PRIMARY KEY, source TEXT NOT NULL, observed_ms INTEGER NOT NULL,
                reason TEXT NOT NULL, start_offset INTEGER, end_offset INTEGER, sample_hex TEXT);
              CREATE TABLE IF NOT EXISTS log_records(source TEXT NOT NULL,generation TEXT NOT NULL,
                start_offset INTEGER NOT NULL,end_offset INTEGER NOT NULL,event_ms INTEGER NOT NULL,
                status INTEGER NOT NULL,request_time_us INTEGER NOT NULL,read_at_ms INTEGER NOT NULL,
                raw_sha256 TEXT NOT NULL,PRIMARY KEY(source,generation,start_offset));
            ''')

    def _fault(self, step):
        if self._fault_hook:
            self._fault_hook(step)

    def attach(self, source_id, host, path, *, start_ms, mode='live',profile=PROFILE,settings=None):
        """Bind a pre-registered, unused external model with the exact log profile."""
        _identifier(source_id, 'source_id')
        reader = LogTailer(path, source_id=source_id, mode=mode)
        aggregate = (PreciseLogAggregator if profile==PRECISE_PROFILE else LogAggregator)(start_ms)
        checkpoint = canonical(pack(reader, aggregate))
        unpack(json.loads(checkpoint))
        config = canonical(configuration(profile,settings))
        with self.store.lock, self.store.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT 1 FROM log_sources WHERE id=? OR host=?', (source_id, host)).fetchone():
                raise Conflict('Источник или модель уже подключены к журналу.')
            info = db.execute('SELECT * FROM hosts WHERE id=?', (host,)).fetchone()
            if info is None:
                raise KeyError('Сначала зарегистрируйте новую модель журнала.')
            state = json.loads(info['state'])
            if (info['source'] != 'external' or info['last_ts'] is not None or state['seen'] != 0 or
                    state['features'] != list(FEATURES) or state.get('processing_mode') != (LOG_MODE if profile==PRECISE_PROFILE else MODE) or
                    state['config']['components'] != 1 or state['lengths'] != [256, 128, 512]):
                raise ValueError('Нужна новая внешняя модель с профилем журнала, без прежних наблюдений.')
            if profile==PRECISE_PROFILE and state['log_settings']!=json.loads(config)['log_settings']:raise ValueError('Параметры источника и модели различаются.')
            db.execute('INSERT INTO log_sources VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                       (source_id, host, str(reader.path), reader.generation, config, digest(config),
                        digest(info['state']), 'active', None, None, utc()))
            db.execute('INSERT INTO log_checkpoints VALUES(?,?,?)', (source_id, checkpoint, digest(checkpoint)))
        return self.status(source_id)

    def _source(self, db, source_id):
        source = db.execute('SELECT * FROM log_sources WHERE id=?', (source_id,)).fetchone()
        if source is None:
            raise KeyError('Источник журнала не найден.')
        return source

    def _load(self, db, source):
        cp = db.execute('SELECT * FROM log_checkpoints WHERE source=?', (source['id'],)).fetchone()
        try:
            config=json.loads(source['config'])
            profile=config.get('profile')
            settings=config.get('log_settings',{})
            options={key:settings[key] for key in ('handler_limit_ms','allowed_error_fraction')} if profile==PRECISE_PROFILE else None
            if (digest(source['config']) != source['config_sha256'] or
                    source['config'] != canonical(configuration(profile,options)) or cp is None or digest(cp['state']) != cp['sha256']):
                raise ValueError('Checksum or configuration mismatch')
            reader, aggregate = unpack(json.loads(cp['state']))
            if isinstance(aggregate,PreciseLogAggregator)!=(profile==PRECISE_PROFILE):raise ValueError('Checkpoint/profile mismatch')
            if (reader.source_id != source['id'] or reader.generation != source['generation'] or
                    str(reader.path) != source['path']):
                raise ValueError('Source identity mismatch')
            return reader, aggregate
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            raise LogIngestError('checkpoint_or_configuration_mismatch') from exc

    def _diagnostic(self, db, source, now_ms, reason, event=None):
        db.execute('INSERT INTO log_ingest_errors(source,observed_ms,reason,start_offset,end_offset,sample_hex) VALUES(?,?,?,?,?,?)',
                   (source, now_ms, reason, event.start_offset if event else None,
                    event.end_offset if event else None, event.sample.hex() if event else None))
        db.execute('DELETE FROM log_ingest_errors WHERE source=? AND id NOT IN '
                   '(SELECT id FROM log_ingest_errors WHERE source=? ORDER BY id DESC LIMIT ?)',
                   (source, source, MAX_DIAGNOSTICS))

    def _block(self, db, source_id, now_ms, reason):
        db.execute("UPDATE log_sources SET status='needs_restart',reason=? WHERE id=?", (reason, source_id))
        self._diagnostic(db, source_id, now_ms, reason)

    def poll(self, source_id, *, now_ms, read_clock_ms=None):
        checked_int(now_ms, 'now_ms', maximum=MAX_TIME_MS-1)
        problem = None
        precise_savepoint=False
        with self.store.lock, self.store.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            source = self._source(db, source_id)
            if source['status'] == 'needs_restart':
                raise LogIngestError('source_needs_restart')
            try:
                reader, aggregate = self._load(db, source)
                precise=isinstance(aggregate,PreciseLogAggregator)
                if precise:db.execute('SAVEPOINT precise_batch');precise_savepoint=True
                count = db.execute("SELECT count(*) FROM log_windows WHERE source=? AND status='ready'", (source_id,)).fetchone()[0]
                capacity = MAX_READY_WINDOWS-count
                if capacity <= 0:
                    return dict(status='backpressure', ready=count, bytes_consumed=0)
                batch = reader.poll(now_ms, read_clock_ms=read_clock_ms)
                # Use actual post-read availability for aggregation AND closing;
                # the cycle-start sample can precede newly appended requests.
                now_ms = batch.read_completed_at_ms
                self._fault('after_read')
                for event in batch.events:
                    if event.kind == 'skipped_startup_tail':
                        self._diagnostic(db, source_id, now_ms, event.kind, event)
                        continue
                    if event.kind == 'line_too_long':
                        aggregate.note_parse_error('line_too_long', read_at_ms=event.read_at_ms)
                        self._diagnostic(db, source_id, now_ms, 'line_too_long', event)
                        continue
                    try:
                        record = (parse_precise_line if precise else parse_line)(event.data)
                    except LogParseError as exc:
                        aggregate.note_parse_error(exc.code, read_at_ms=event.read_at_ms)
                        self._diagnostic(db, source_id, now_ms, exc.code, event)
                    else:
                        outcome = aggregate.add(record, read_at_ms=event.read_at_ms)
                        if precise:
                            db.execute('INSERT INTO log_records VALUES(?,?,?,?,?,?,?,?,?)',
                                (source_id,reader.generation,event.start_offset,event.end_offset,record.timestamp_ms,
                                 record.status,record.request_time_us,event.read_at_ms,hashlib.sha256(event.data).hexdigest()))
                        if outcome == 'late':
                            self._diagnostic(db, source_id, now_ms, 'late_record', event)
                # Drain already-readable lines before finalizing potentially incomplete counts.
                # Future polls still use their real availability times; no backdating.
                windows = [] if batch.backlog_bytes else aggregate.advance(now_ms, max_windows=min(MAX_EMIT_WINDOWS, capacity))
                for window in windows:
                    payload = canonical(window.as_dict())
                    db.execute("INSERT INTO log_windows VALUES(?,?,?,?,?,'ready',NULL,NULL)",
                               (source_id, reader.generation, window.start_ms, payload, digest(payload)))
                self._fault('after_windows')
                checkpoint = canonical(pack(reader, aggregate))
                unpack(json.loads(checkpoint))
                db.execute('UPDATE log_checkpoints SET state=?,sha256=? WHERE source=?',
                           (checkpoint, digest(checkpoint), source_id))
                status = 'degraded' if aggregate.requires_restart else 'active'
                db.execute('UPDATE log_sources SET status=?,reason=? WHERE id=?',
                           (status, 'data_quality' if aggregate.requires_restart else None, source_id))
                self._fault('after_checkpoint')
                result = dict(status=status, events=len(batch.events), bytes_consumed=batch.bytes_consumed,
                              windows_saved=len(windows), read_offset=batch.read_offset,
                              backlog_bytes=batch.backlog_bytes, closing_deferred=bool(batch.backlog_bytes))
                if precise_savepoint:db.execute('RELEASE precise_batch');precise_savepoint=False
            except (TailerError, AggregationError, LogIngestError) as exc:
                if precise_savepoint:db.execute('ROLLBACK TO precise_batch');db.execute('RELEASE precise_batch')
                problem = exc.code
                self._block(db, source_id, now_ms, problem)
        if problem:
            raise LogIngestError(problem)
        self._fault('after_poll_commit')
        return result

    def deliver(self, source_id, *, now_ms, start_ms=None, expected_sha256=None):
        """Retry by source/session/start plus identical hash, never by HTTP 409."""
        checked_int(now_ms, 'now_ms', maximum=MAX_TIME_MS-1)
        if start_ms is not None:
            checked_int(start_ms, 'start_ms', maximum=MAX_TIME_MS-1)
        problem = None
        with self.store.lock, self.store.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            source = self._source(db, source_id)
            if source['status'] == 'needs_restart':
                raise LogIngestError('source_needs_restart')
            row = (db.execute("SELECT * FROM log_windows WHERE source=? AND generation=? AND status='ready' ORDER BY start_ms LIMIT 1",
                              (source_id, source['generation'])).fetchone() if start_ms is None else
                   db.execute('SELECT * FROM log_windows WHERE source=? AND generation=? AND start_ms=?',
                              (source_id, source['generation'], start_ms)).fetchone())
            if row is None:
                return dict(status='nothing_ready')
            try:
                _, aggregate = self._load(db, source)
                if digest(row['payload']) != row['sha256'] or (expected_sha256 is not None and expected_sha256 != row['sha256']):
                    raise LogIngestError('window_hash_mismatch')
                try:
                    window = window_from_dict(json.loads(row['payload']))
                except (ValueError, KeyError, TypeError) as exc:
                    raise LogIngestError('invalid_saved_window') from exc
                if window.start_ms != row['start_ms']:
                    raise LogIngestError('window_identity_mismatch')
                if window.start_ms < aggregate._start_ms or window.end_ms > aggregate._next_start_ms:
                    raise LogIngestError('window_outside_committed_checkpoint')
                if now_ms < window.finalized_at_ms or (source['last_delivery_ms'] is not None and now_ms < source['last_delivery_ms']):
                    raise LogIngestError('delivery_clock_went_backwards')
                if row['status'] != 'ready':
                    if row['receipt'] is None or digest(row['receipt']) != row['receipt_sha256']:
                        raise LogIngestError('delivery_receipt_mismatch')
                    if row['status'] == 'delivered':
                        obs = db.execute('SELECT payload FROM observations WHERE host=? AND ts=?',
                                         (source['host'], window.end_ms/1000)).fetchone()
                        if obs is None or canonical(json.loads(obs['payload'])) != canonical(window.metrics()):
                            raise LogIngestError('delivered_observation_mismatch')
                    result = dict(status='already_'+row['status'], receipt=json.loads(row['receipt']), sha256=row['sha256'])
                else:
                    first = db.execute("SELECT min(start_ms) FROM log_windows WHERE source=? AND status='ready'", (source_id,)).fetchone()[0]
                    if first != row['start_ms']:
                        raise Conflict('Сначала доставьте более ранний интервал.')
                    if window.eligible_for_model:
                        info = db.execute('SELECT state FROM hosts WHERE id=?', (source['host'],)).fetchone()
                        if info is None or digest(info['state']) != source['host_state_sha256']:
                            raise LogIngestError('model_changed_outside_log_delivery')
                        receipt = self.store._ingest_in_transaction(db, source['host'], {'observations': [window.as_observation()]})
                        started = receipt.pop('_processing_started')
                        receipt['processing_before_commit_seconds'] = time.perf_counter()-started
                        target = 'delivered'
                        self._fault('after_ingest')
                        current = db.execute('SELECT state FROM hosts WHERE id=?', (source['host'],)).fetchone()['state']
                        db.execute('UPDATE log_sources SET host_state_sha256=? WHERE id=?', (digest(current), source_id))
                    else:
                        receipt = dict(reason=window.quality, accepted=0)
                        target = 'skipped'
                    receipt.update(window_start_ms=window.start_ms, window_end_ms=window.end_ms,
                                   finalized_at_ms=window.finalized_at_ms, delivery_requested_at_ms=now_ms,
                                   transaction_recorded_at=utc())
                    payload = canonical(receipt)
                    db.execute('UPDATE log_windows SET status=?,receipt=?,receipt_sha256=? WHERE source=? AND generation=? AND start_ms=?',
                               (target, payload, digest(payload), source_id, source['generation'], window.start_ms))
                    db.execute('UPDATE log_sources SET last_delivery_ms=? WHERE id=?', (now_ms, source_id))
                    self._fault('after_delivery_mark')
                    result = dict(status=target, receipt=receipt, sha256=row['sha256'])
            except LogIngestError as exc:
                problem = exc.code
                self._block(db, source_id, now_ms, problem)
        if problem:
            raise LogIngestError(problem)
        self._fault('after_delivery_commit')
        result['storage_acknowledged_at'] = utc()  # After COMMIT, not a backdated window timestamp.
        return result

    def status(self, source_id):
        with self.store.lock, self.store.connection() as db:
            source = self._source(db, source_id)
            counts = {row['status']: row['n'] for row in db.execute(
                'SELECT status,count(*) AS n FROM log_windows WHERE source=? GROUP BY status', (source_id,))}
            try:
                reader, aggregate = self._load(db, source)
            except LogIngestError as exc:
                return dict(source=source_id, host=source['host'], generation=source['generation'],
                            status='needs_restart', reason=source['reason'] or exc.code,
                            persisted_status=source['status'], reader=None, aggregate=None,
                            windows=counts, durable_checkpoint=False)
        reader_info=reader.diagnostics()
        reader_info['profile']=json.loads(source['config'])['profile']
        return dict(source=source_id, host=source['host'], generation=source['generation'],
                    status=source['status'], reason=source['reason'], reader=reader_info,
                    aggregate=aggregate.diagnostics(), windows=counts, durable_checkpoint=True)

    def windows(self, source_id, *, limit=256):
        checked_int(limit, 'limit', minimum=1, maximum=256)
        with self.store.lock, self.store.connection() as db:
            self._source(db, source_id)
            rows = db.execute('SELECT start_ms,status,sha256,payload FROM log_windows WHERE source=? ORDER BY start_ms LIMIT ?',
                              (source_id, limit)).fetchall()
        return [dict(start_ms=r['start_ms'], status=r['status'], sha256=r['sha256'], payload=json.loads(r['payload'])) for r in rows]
