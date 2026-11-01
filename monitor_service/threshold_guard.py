"""Eight outputs from ONE unchanged scoring engine; threshold policy only.

No collection or outcomes. Full common history includes flagged observations.
F/U/G decisions use old thresholds, then the boundary updates the NEXT block.
"""
import json
import math
from pathlib import Path
import time
from .cycle_profile import CycleComparison,project
from .threshold_guard_protocol import load_protocol,NAMES,SPEC,sha

VERSION='threshold-guard-implementation-v0.1'
RESOURCE={'ratio':'cycles_per_request','expected':'expected_cycles'}
INTERNAL={'ratio':'cpu_per_request','expected':'expected_cpu'}


class Limits:
    def __init__(self,initial):
        if type(initial) not in (int,float) or not math.isfinite(initial) or initial<=0:
            raise ValueError('Positive finite initial threshold required; no epsilon substitution.')
        self.initial=float(initial);self.updated=float(initial);self.limited=float(initial)

    def values(self):return dict(fixed=self.initial,updated=self.updated,limited=self.limited)

    def advance(self,candidate):
        if type(candidate) not in (int,float) or not math.isfinite(candidate) or candidate<0:
            raise ValueError('Invalid common threshold candidate.')
        self.updated=float(candidate)
        self.limited=min(self.updated,self.limited+.10*self.initial)


class GuardComparison:
    def __init__(self,engine=None):
        load_protocol();self.engine=CycleComparison() if engine is None else engine
        self.limits={};self.failed=None

    def observe(self,raw):
        if self.failed:raise ValueError('Failed comparison: '+self.failed)
        try:return self._observe(raw)
        except Exception as exc:
            self.failed=type(exc).__name__+': '+str(exc);raise

    def _observe(self,raw):
        common=self.engine.observe(raw)
        if common is None:
            if self.engine.simple.seen==896:
                self.limits={prefix:Limits(self.engine.simple.cuts[key]) for prefix,key in INTERNAL.items()}
            return None
        if set(self.limits)!=set(RESOURCE):raise ValueError('Common warmup not complete.')
        post=self.engine.simple.seen-896
        if post<1:raise ValueError('Post-warmup position invalid.')
        before={prefix:limit.values() for prefix,limit in self.limits.items()}
        out={};input_time=common['common_input_preparation_seconds'];window_time=common['common_window_preparation_seconds']
        for new,old in (('ipca_reference','ipca'),('separate_limits_reference','separate_limits')):
            d=dict(common['decisions'][old]);cost=(d['own_compute_seconds'] if old=='ipca' else
                window_time+d['own_score_seconds']+common['simple_update_seconds'][old])
            d.update(base_method=old,variant='reference',availability_cost_seconds=input_time+cost)
            out[new]=d
        for prefix,old in RESOURCE.items():
            original=common['decisions'][old]
            if original['threshold']!=self.limits[prefix].updated:
                raise ValueError('Current row threshold was influenced by its own update.')
            for variant,threshold in before[prefix].items():
                mark=time.perf_counter();score=original['score']
                alarm=None if score is None else bool(score>threshold)
                elapsed=time.perf_counter()-mark
                d=dict(original,threshold=threshold,alarm=alarm,base_method=old,variant=variant,
                       own_policy_decision_seconds=elapsed,own_policy_update_seconds=0.)
                out[prefix+'_'+variant]=d
        boundary=post%16==0
        update_cost={prefix:0. for prefix in RESOURCE}
        if boundary:
            for prefix,key in INTERNAL.items():
                mark=time.perf_counter();self.limits[prefix].advance(self.engine.simple.cuts[key])
                update_cost[prefix]=time.perf_counter()-mark
        else:
            for prefix,key in INTERNAL.items():
                if self.engine.simple.cuts[key]!=self.limits[prefix].updated:
                    raise ValueError('Threshold changed away from a 16-row boundary.')
        for prefix,old in RESOURCE.items():
            for variant in ('fixed','updated','limited'):
                d=out[prefix+'_'+variant]
                d['own_policy_update_seconds']=update_cost[prefix] if variant=='limited' else 0.
                d['availability_cost_seconds']=(input_time+window_time+d['own_score_seconds']+
                    (common['simple_update_seconds'][old] if variant!='fixed' else 0.)+
                    d['own_policy_decision_seconds']+d['own_policy_update_seconds'])
        if set(out)!=set(NAMES):raise ValueError('Incomplete variant outputs.')
        return dict(version=VERSION,timestamp=common['timestamp'],decisions=out,
            common=common,post_row=post,block_updated=boundary,thresholds_before=before,
            thresholds_after={prefix:limit.values() for prefix,limit in self.limits.items()},
            cost_scope='Shared relevant calculation plus own policy sections; composite, not eight independent programs.')

    def state(self):
        return dict(version=VERSION,core=self.engine.to_state(),limits={k:v.values() for k,v in self.limits.items()},failed=self.failed)


def dump(path,value):
    with Path(path).open('x',encoding='utf-8') as stream:json.dump(value,stream,allow_nan=False,separators=(',',':'))


def source_hashes():
    base=Path(__file__).resolve().parent
    return {name:sha(base/name) for name in ('threshold_guard.py','threshold_guard_protocol.py',
        'threshold_guard_schedule.py','threshold_guard_measurements.py','threshold_guard_evaluation.py',
        'cycle_profile.py','efficiency_baselines.py','immediate.py','detector.py','_reference_models.py')}


def save_outputs(counters,output):
    output=Path(output).resolve();output.mkdir(parents=True,exist_ok=False)
    engine=GuardComparison();streams={};inputs=count=0
    dump(output/'metadata.json',dict(version=VERSION,protocol_sha256=sha(SPEC),source_sha256=source_hashes(),
        warmup_rows=896,names=list(NAMES),no_outcomes=True,quality_evaluated=False))
    try:
        for name in (*NAMES,'comparison_trace','projected_measurements'):
            streams[name]=(output/(name+'.jsonl')).open('x',encoding='utf-8')
        previous_arrival=None
        for raw in counters:
            clean=project(raw);arrival=raw.get('collector_received_at');inputs+=1
            if inputs>1:
                if (type(arrival) not in (int,float) or not math.isfinite(arrival) or arrival<clean['timestamp']
                        or (previous_arrival is not None and arrival<=previous_arrival)):
                    raise ValueError('Invalid saved acquisition timestamp.')
                previous_arrival=arrival
            streams['projected_measurements'].write(json.dumps(dict(clean,collector_received_at=arrival),allow_nan=False)+'\n')
            result=engine.observe(clean)
            if result is None:continue
            count+=1;result['collector_received_at']=arrival
            for name in NAMES:
                record=dict(result['decisions'][name],timestamp=result['timestamp'],collector_received_at=arrival,version=VERSION)
                streams[name].write(json.dumps(record,allow_nan=False)+'\n')
            streams['comparison_trace'].write(json.dumps(result,allow_nan=False)+'\n')
        if count<1:raise ValueError('No complete post-warmup outputs.')
        for stream in streams.values():stream.close()
        dump(output/'comparison_state.json',engine.state())
        dump(output/'scoring_status.json',dict(status='complete',input_rows=inputs,post_rows=count,quality_evaluated=False))
        paths=sorted(p for p in output.iterdir() if p.is_file())
        dump(output/'eight_outputs_complete.json',dict(version=VERSION,protocol_sha256=sha(SPEC),
            input_rows=inputs,post_rows=count,quality_evaluated=False,
            files=[dict(path=p.name,bytes=p.stat().st_size,sha256=sha(p)) for p in paths]))
        return verify_outputs(output)
    except BaseException:
        for stream in streams.values():stream.close()
        marker=output/'eight_outputs_complete.json'
        if marker.exists():marker.unlink() # Only a marker in this newly created output folder.
        raise


def verify_outputs(output):
    output=Path(output).resolve();load_protocol()
    barrier=json.loads((output/'eight_outputs_complete.json').read_text(encoding='utf-8'))
    required={name+'.jsonl' for name in (*NAMES,'comparison_trace','projected_measurements')}|{'metadata.json','comparison_state.json','scoring_status.json'}
    if (barrier.get('version')!=VERSION or barrier.get('protocol_sha256')!=sha(SPEC) or
            {r['path'] for r in barrier['files']}!=required or len(barrier['files'])!=len(required) or
            barrier['post_rows']<1 or barrier['input_rows']!=barrier['post_rows']+897):
        raise ValueError('Invalid eight-output barrier.')
    for record in barrier['files']:
        path=(output/record['path']).resolve()
        if not path.is_relative_to(output) or path.stat().st_size!=record['bytes'] or sha(path)!=record['sha256']:
            raise ValueError('Output changed after scoring.')
    metadata=json.loads((output/'metadata.json').read_text(encoding='utf-8'))
    if metadata['source_sha256']!=source_hashes():raise ValueError('Scoring implementation changed.')
    for name in (*NAMES,'comparison_trace'):
        with (output/(name+'.jsonl')).open(encoding='utf-8') as stream:
            if sum(1 for _ in stream)!=barrier['post_rows']:raise ValueError('Incomplete row coverage.')
    def rows(name):
        with (output/(name+'.jsonl')).open(encoding='utf-8') as stream:return [json.loads(line) for line in stream]
    projected=rows('projected_measurements');trace=rows('comparison_trace')
    if len(projected)!=barrier['input_rows']:raise ValueError('Incomplete projected input.')
    expected=[r['timestamp'] for r in projected[897:]]
    if [r['timestamp'] for r in trace]!=expected:raise ValueError('Trace timestamps differ from inputs.')
    for name in NAMES:
        saved=rows(name)
        if [r['timestamp'] for r in saved]!=expected:raise ValueError('Variant timestamps differ.')
        for record,t in zip(saved,trace):
            if (record['collector_received_at']!=t['collector_received_at'] or
                    any(record.get(k)!=v for k,v in t['decisions'][name].items())):
                raise ValueError('Variant differs from common trace.')
    return barrier
