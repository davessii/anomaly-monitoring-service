"""Versioned extension: diverse normal warmup, unchanged fault scenario.

The original validation_scenario and its 51-file seal remain untouched.
"""
import argparse
import json
from pathlib import Path

from .validation_scenario import (ROOT, Api, Evidence, Scenario, save, schedule,
                                  sha, verify_bundle as verify_original)
from .warmup_load import NormalLoadPlan

BUNDLE=Path(__file__).with_name('validation_multiload_v2')
RESULTS=ROOT/'outputs/service-validation-multiload-v2'


def load_protocol():
    return json.loads((BUNDLE/'protocol.json').read_text(encoding='utf-8'))


def verify_bundle():
    original=verify_original()
    seal=json.loads((BUNDLE/'seal.json').read_text(encoding='utf-8'))
    if seal['id']!='service-validation-multiload-v2' or seal['original_seal_sha256']!=original:
        raise ValueError('Original service or versioned warmup seal changed.')
    for name,digest in seal['files'].items():
        path=(ROOT/name).resolve()
        if not path.is_relative_to(ROOT) or not path.is_file() or sha(path)!=digest:
            raise ValueError('Changed multiload input: '+name)
    return sha(BUNDLE/'seal.json')


class MultiLoadScenario(Scenario):
    def warmup(self):
        self.phase='warmup'
        started=self.clock.monotonic()
        settings=self.protocol['source_config']['log_settings']
        app=self.command('/api/bench/start',{})
        command=self.commands[-1]
        if (app['id'] in self.preexisting_app_ids or not app.get('running')
                or type(app.get('started_at')) not in (int,float)
                or not command['sent']['utc']<=app['started_at']<=command['acknowledged']['utc']):
            raise ValueError('A fresh owned application was not acknowledged.')
        self.run,self.filename=app['id'],app['access_log_file']
        if app.get('access_log_profile')!=self.protocol['source_config']['profile']:
            raise ValueError('Unexpected access log profile.')
        source=self.command('/api/logs/start',dict(filename=self.filename,
            profile=self.protocol['source_config']['profile'],handler_limit_ms=settings['handler_limit_ms'],
            allowed_error_fraction=settings['allowed_error_fraction']))
        self.source=source['id']
        if source['host']!=self.source or self.source==self.run:
            raise ValueError('Separate fresh log model required.')
        save(self.output/'runtime.json',self.evidence.runtime(self.run,self.protocol))
        plan=NormalLoadPlan(**self.protocol['warmup_load']['plan_arguments'])
        save(self.output/'warmup_plan.json',dict(steps=plan.describe(),
            after_plan='2 rps until both original readiness checks pass',
            fit_reference_validation_rows=self.protocol['source_config']['lengths']))
        origin=self.clock.stamp()
        last=-1
        while self.clock.monotonic()-started<self.protocol['generator']['warmup_timeout_seconds']:
            elapsed=self.clock.monotonic()-origin['monotonic']
            step=plan.at(elapsed)
            if step['index']!=last:
                if step['index']!=last+1:
                    raise RuntimeError('A prescribed warmup slot was skipped; no restart.')
                late=elapsed-step['planned_start']
                if late>self.protocol['warmup_load']['max_switch_lateness_seconds']:
                    raise RuntimeError('Warmup load switch exceeded its fixed lateness limit.')
                # 120 seconds prevents accidental fallback before the next planned
                # 64-second switch. Every switch, including final return to 2, is explicit.
                self.command('/api/bench/load',dict(rate=step['rate'],seconds=120))
                self.events.add('warmup_load_applied',**step,elapsed_seconds=elapsed,
                                lateness_seconds=late,origin=origin)
                last=step['index']
            ready=self.health()
            if ready and step['curriculum_complete']:
                save(self.output/'model_before.json',self.evidence.check_model(self.source,self.protocol))
                self.events.add('warmup_complete',elapsed_seconds=self.clock.monotonic()-started,
                                curriculum_complete=True,load_plan=plan.describe())
                return
            self.clock.sleep(self.protocol['generator']['poll_seconds'])
        raise TimeoutError('Full diverse-load warmup did not finish in 1200 seconds; no retry.')


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute',action='store_true')
    parser.add_argument('--run',type=int,choices=(6,7))
    parser.add_argument('--service-url',default='http://127.0.0.1:8765')
    parser.add_argument('--data-dir',type=Path,default=Path(__file__).with_name('data'))
    args=parser.parse_args(argv)
    if args.execute and args.run is None:
        parser.error('--execute requires --run 6 or 7.')
    protocol=load_protocol()
    digest=verify_bundle()
    if not args.execute:
        print(json.dumps(dict(action='describe_only',real_recordings_started=0,
            protocol_seal_sha256=digest,warmup=NormalLoadPlan().describe(),
            schedules={str(n):schedule(protocol,n) for n in ([args.run] if args.run else (6,7))}),indent=2))
        return 0
    runner=MultiLoadScenario(protocol,args.run,Api(args.service_url,protocol['generator']['http_timeout_seconds']),
        Evidence(args.data_dir),RESULTS/f'run-{args.run:02d}',check_bundle=verify_bundle)
    result=runner.execute()
    print(json.dumps(dict(status=result['status'],quality_evaluated=False,folder=str(runner.output)),indent=2))
    return 0 if result['status']=='recorded_not_evaluated' else 1


if __name__=='__main__':
    raise SystemExit(main())
