"""Proposed hidden-degradation experiment. Default describes; never starts work.

Only a later explicit --execute starts a fresh loopback app, client and log reader.
Durations in access.log are measured, never replaced by the prescribed delay.
"""
import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
from pathlib import Path
import statistics
import threading
import time
import urllib.request

from .validation_scenario import ROOT, Api, Evidence, save, sha
from .log_access_writer import AccessLogWriter
from .warmup_load import NormalLoadPlan

BUNDLE=Path(__file__).with_name('hidden_degradation_v2')
OUTPUT=ROOT/'outputs/hidden-degradation-v2'


def protocol():
    return json.loads((BUNDLE/'protocol.json').read_text(encoding='utf-8'))


class HiddenPlan:
    """Request-start assignment; phase labels go ONLY to the audit journal."""
    def at(self, elapsed):
        if not math.isfinite(elapsed) or elapsed<0:
            raise ValueError('Nonnegative finite elapsed time required.')
        if elapsed<30:return dict(phase='reference',rate=2,extra_ns=0)
        if elapsed<60:
            return dict(phase='hidden_growth',rate=2,
                        extra_ns=int(1_700_000*(elapsed-30)/30))
        return dict(phase='recovery' if elapsed<80 else 'drain',rate=2,extra_ns=0)


class RelativeRule:
    """Fixed factor-three threshold from the same initial fit data, at low load."""
    def __init__(self, initial_fit):
        if len(initial_fit)!=256:
            raise ValueError('Exactly the first 256 eligible fit windows required.')
        if any(not math.isfinite(x['mean_request_ms']) or x['mean_request_ms']<=0
               or x['mean_request_ms']>10 or x['server_error_fraction']!=0 for x in initial_fit):
            raise ValueError('Initial fit must have positive finite healthy response times.')
        values=[x['mean_request_ms'] for x in initial_fit if 1<=x['completed_rps']<=3]
        if len(values)<64:
            raise ValueError('At least 64 ordinary low-load fit windows required.')
        self.reference_ms=statistics.median(values)
        self.threshold_ms=3*self.reference_ms

    def alarm(self,mean_ms):
        return mean_ms>self.threshold_ms


def absolute_alarm(mean_ms):
    return mean_ms>10


def bounded_wait(nanoseconds,now=time.perf_counter_ns):
    # A sub-2ms deadline avoids pretending that sleep() has sub-ms precision.
    # At 2 requests/s this is at most 3.4ms of prescribed waiting per second.
    if type(nanoseconds) is not int or not 0<=nanoseconds<=1_700_000:
        raise ValueError('Extra wait must be 0..1700000 ns.')
    start=now()
    if nanoseconds:
        while now()-start<nanoseconds:
            pass
    return now()-start


class Audit:
    def __init__(self,path):
        self.stream=path.open('x',encoding='utf-8');self.lock=threading.Lock()

    def add(self,kind,**values):
        with self.lock:
            self.stream.write(json.dumps(dict(kind=kind,utc=time.time(),monotonic=time.monotonic(),
                                             **values),allow_nan=False,ensure_ascii=False)+'\n')
            self.stream.flush()

    def close(self):self.stream.close()


class WorkHandler(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    def log_message(self,*args):pass
    def setup(self):super().setup();self.connection.settimeout(3)

    def do_GET(self):
        if self.headers.get('Host')!=f'127.0.0.1:{self.server.server_port}' or self.path!='/work':
            self.send_error(404);return
        started=time.perf_counter_ns();started_utc=time.time();started_mono=time.monotonic()
        item=self.server.plan_for(started_mono)
        status=0;actual_wait=0
        try:
            value=hashlib.sha256(b'thesis-workbench').digest()
            for _ in range(128):value=hashlib.sha256(value).digest()
            actual_wait=bounded_wait(item['extra_ns'])
            body=json.dumps({'result':value.hex()}).encode()
            self.send_response(200);self.send_header('Content-Length',str(len(body)))
            self.send_header('Content-Type','application/json');self.send_header('Connection','close')
            self.end_headers();self.wfile.write(body);self.wfile.flush();status=200
        except (OSError,TimeoutError):pass
        finally:
            self.close_connection=True
            duration=time.perf_counter_ns()-started
            self.server.access.record(status,duration)
            self.server.audit.add('request_handler',started_utc=started_utc,started_monotonic=started_mono,
                completed_utc=time.time(),phase=item['phase'],prescribed_extra_ns=item['extra_ns'],
                actual_wait_ns=actual_wait,measured_handler_ns=duration,status=status)


class WorkApplication(ThreadingHTTPServer):
    daemon_threads=False
    def __init__(self,access,audit):
        super().__init__(('127.0.0.1',0),WorkHandler)
        self.access,self.audit=access,audit
        self.origin=None;self.plan=HiddenPlan()

    def plan_for(self,monotonic):
        return (dict(phase='warmup',rate=2,extra_ns=0) if self.origin is None
                else self.plan.at(monotonic-self.origin['monotonic']))


def write_command(api,audit,path,body):
    sent=dict(utc=time.time(),monotonic=time.monotonic())
    result=api.call(path,body)
    audit.add('command',path=path,body=body,sent=sent,
              acknowledged=dict(utc=time.time(),monotonic=time.monotonic()))
    return result


def emit_load(app,stop,get_rate,audit):
    """A bounded sequential loopback client; delayed clocks never create a burst."""
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    intervals=(.37,.61,.43,.59);index=0;deadline=time.monotonic()
    while not stop.is_set():
        if stop.wait(max(0,deadline-time.monotonic())):break
        sent=time.time();status=None;error=None
        try:
            with opener.open(f'http://127.0.0.1:{app.server_port}/work',timeout=2) as response:
                status=response.status;response.read()
        except Exception as exc:error=type(exc).__name__
        audit.add('client_request',sent_utc=sent,received_utc=time.time(),status=status,error=error)
        step=intervals[index%4]*2/get_rate();index+=1
        deadline+=step
        if deadline<time.monotonic():
            audit.add('client_schedule_overrun',skipped_burst=True)
            deadline=time.monotonic()+step


def source_health(api,source,expected_config):
    row=next((x for x in api.call('/api/logs')['sources'] if x['source']==source),None)
    if (not row or row['status']!='active' or not row['worker']['running']
            or row['worker'].get('error') or row['model']['phase']=='unfit'):
        raise RuntimeError('Log source failed or initialization is unfit; no retry.')
    if row['config']!=expected_config:
        raise RuntimeError('Saved source configuration changed.')
    return row['model']


def fit_metrics(evidence,source):
    with closing(evidence.connect(evidence.monitor)) as db:
        rows=db.execute('SELECT payload FROM observations WHERE host=? ORDER BY ts LIMIT 256',(source,)).fetchall()
    return [json.loads(row['payload']) for row in rows]


def export_source(evidence,source,output):
    with closing(evidence.connect(evidence.monitor)) as db:
        for name,query in (
            ('observations','SELECT * FROM observations WHERE host=? ORDER BY ts'),
            ('messages','SELECT * FROM log_messages WHERE host=? ORDER BY ts'),
            ('windows','SELECT * FROM log_windows WHERE source=? ORDER BY start_ms')):
            Evidence.rows(db,output/(name+'.jsonl'),query,source)
    save(output/'model_after.json',evidence.model(source))


def verify_design():
    from .validation_multiload import verify_bundle
    inherited=verify_bundle()
    seal=json.loads((BUNDLE/'seal.json').read_text(encoding='utf-8'))
    if inherited!=seal['inherited_seal_sha256']:raise ValueError('The fixed service changed.')
    for name,digest in seal['files'].items():
        if sha(ROOT/name)!=digest:raise ValueError('Proposed design changed: '+name)
    return sha(BUNDLE/'seal.json')


def execute(args):
    p=protocol();seal=verify_design();output=OUTPUT/f'run-{args.run:02d}'
    api=Api(args.service_url,3);evidence=Evidence(args.data_dir)
    # No old app or log source is reused or stopped.
    if api.call('/api/health').get('version')!='0.7.0':raise ValueError('Expected service 0.7.0.')
    if any(x['worker']['running'] for x in api.call('/api/logs')['sources']):
        raise ValueError('Other log observations are active.')
    if any(x.get('running') for x in api.call('/api/state')['jobs']):
        raise ValueError('Another collector is active.')
    current=api.call('/api/bench').get('current')
    if current and current['running']:raise ValueError('Another owned application is active.')
    log_dir=(args.data_dir/'log_inputs').resolve()
    if not log_dir.is_dir():raise ValueError('Existing default service log_inputs directory required.')
    output.mkdir(parents=True,exist_ok=False)
    save(output/'attempt.json',dict(protocol=p['id'],run=args.run,protocol_seal_sha256=seal,
                                  started_at_utc=datetime.now(timezone.utc).isoformat(),quality_evaluated=False))
    audit=Audit(output/'scenario_events.jsonl');source=None;app=None;thread=None;client=None
    stop=threading.Event();rate=[2];writer=None;status='invalid';error=None;origin=None
    try:
        filename=f'hidden-degradation-{args.run:02d}-{time.time_ns()}.log'
        writer=AccessLogWriter(log_dir/filename,profile='owned_access_us_v2')
        app=WorkApplication(writer,audit);thread=threading.Thread(target=app.serve_forever,daemon=True);thread.start()
        connected=write_command(api,audit,'/api/logs/start',dict(filename=filename,
            profile='owned_access_us_v2',handler_limit_ms=10,allowed_error_fraction=0))
        source=connected['id']
        if connected['host']!=source:raise ValueError('Separate fresh log model required.')
        client=threading.Thread(target=emit_load,args=(app,stop,lambda:rate[0],audit),daemon=True);client.start()
        plan=NormalLoadPlan();start=time.monotonic();last=-1
        while time.monotonic()-start<1200:
            elapsed=time.monotonic()-start;step=plan.at(elapsed)
            if step['index']!=last:
                if step['index']!=last+1 or elapsed-step['planned_start']>2:
                    raise RuntimeError('Warmup schedule late or skipped; no replay.')
                rate[0]=step['rate'];last=step['index'];audit.add('warmup_load',**step,elapsed_seconds=elapsed)
            model=source_health(api,source,p['source_config'])
            # Fixed 64-second low-load settling after the 896-second curriculum.
            if elapsed>=960 and model['ready']:break
            time.sleep(1)
        else:raise TimeoutError('Warmup incomplete after 1200 seconds.')
        before=evidence.check_model(source,p)
        baseline=RelativeRule(fit_metrics(evidence,source))
        save(output/'model_before.json',before)
        save(output/'baselines_before.json',dict(absolute_threshold_ms=10,
            relative_reference_ms=baseline.reference_ms,relative_threshold_ms=baseline.threshold_ms,
            relative_factor=3,fit_rows=256,selection='fit rows with 1..3 completed_rps',threshold_updates=False))
        if baseline.threshold_ms>=2:
            # Record this limitation before the test, without changing the threshold.
            audit.add('baseline_range_limitation',relative_threshold_ms=baseline.threshold_ms)
        assert verify_design()==seal
        origin=dict(utc=time.time(),monotonic=time.monotonic());app.origin=origin;rate[0]=2
        audit.add('scenario_begin',origin=origin)
        last_phase=None
        while time.monotonic()-origin['monotonic']<84:
            elapsed=time.monotonic()-origin['monotonic'];item=app.plan.at(elapsed)
            if item['phase']!=last_phase:
                expected={'reference':0,'hidden_growth':30,'recovery':60,'drain':80}[item['phase']]
                if elapsed-expected>1:raise RuntimeError('Scenario boundary observation late; no retry.')
                audit.add('phase_boundary_observed',phase=item['phase'],planned_seconds=expected,
                          observed_seconds=elapsed);last_phase=item['phase']
            if not source_health(api,source,p['source_config'])['ready']:
                raise RuntimeError('Model readiness was lost during the scenario.')
            if not client.is_alive():raise RuntimeError('Request generator stopped.')
            stamp=time.time()
            if abs((stamp-origin['utc'])-(time.monotonic()-origin['monotonic']))>1:
                raise RuntimeError('UTC and monotonic clocks drifted.')
            time.sleep(.2)
        assert verify_design()==seal
        status='recorded_not_evaluated'
    except (Exception,KeyboardInterrupt) as exc:
        error=type(exc).__name__+': '+str(exc);audit.add('technical_failure',error=error)
    finally:
        try:
            if source:
                write_command(api,audit,'/api/logs/stop',dict(source_id=source))
                deadline=time.monotonic()+15
                while any(x['source']==source and x['worker']['running'] for x in api.call('/api/logs')['sources']):
                    if time.monotonic()>deadline:raise TimeoutError('Log reader did not stop.')
                    time.sleep(.2)
        except Exception as exc:status='invalid';error=(error or '')+'; reader cleanup: '+str(exc)
        stop.set()
        if client:client.join(timeout=3)
        if app:
            if thread and thread.is_alive():app.shutdown()
            app.server_close()
        if thread:thread.join(timeout=3)
        if writer:
            writer.close()
            with (output/'access.log').open('xb') as stream:stream.write(writer.path.read_bytes())
        try:
            if source:export_source(evidence,source,output)
        except Exception as exc:status='invalid';error=(error or '')+'; export: '+str(exc)
        audit.add('recording_closed',status=status,quality_evaluated=False);audit.close()
        save(output/'run_manifest.json',dict(protocol=p['id'],protocol_seal_sha256=seal,
            run=args.run,status=status,error=error,source=source,origin=origin,quality_evaluated=False,
            file_sha256={x.name:sha(x) for x in output.iterdir() if x.is_file()}))
    return dict(status=status,folder=str(output),quality_evaluated=False)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute',action='store_true')
    parser.add_argument('--run',type=int,choices=(8,9))
    parser.add_argument('--service-url',default='http://127.0.0.1:8765')
    parser.add_argument('--data-dir',type=Path,default=Path(__file__).with_name('data'))
    args=parser.parse_args(argv)
    if args.execute and args.run is None:parser.error('--execute requires --run 8 or 9.')
    if not args.execute:
        print(json.dumps(dict(action='describe_only',real_recordings_started=0,protocol=protocol()['id'],
            schedule=protocol()['scenario'],extra_delay_is_added_not_total_latency=True),ensure_ascii=False,indent=2))
        return 0
    result=execute(args);print(json.dumps(result,ensure_ascii=False,indent=2))
    return 0 if result['status']=='recorded_not_evaluated' else 1


if __name__=='__main__':raise SystemExit(main())
