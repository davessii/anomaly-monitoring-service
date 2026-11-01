"""Closed collection -> whitelist export -> measurement gate -> four outputs -> quality.

No collection on import or from this CLI. Quality cannot open outcomes before
the measurement admission and all four output files have been verified.
"""
import argparse
from contextlib import contextmanager
from dataclasses import asdict
import json
import math
from pathlib import Path
import sqlite3
import statistics
import zipfile
from .cycle_profile import PROFILE, FEATURES, project, check_pair, CycleWindow, detector_values
from .cycle_outputs import save_outputs, verify_outputs, NAMES, encode, sha
from .efficiency_schedule import post_schedule

VERSION='cycle-processing-v0.3'
RULES={'version':VERSION,'interior_exclude_first_seconds':3,'interior_exclude_last_seconds':2,
       'eligible_fraction_min_each_ordinary_segment':.9,'positive_fraction_min_each_ordinary_segment':.9,
       'disjoint_eligible_windows_min_each_ordinary_segment':2,'read_width_max_fraction':.05,
       'passing_read_width_fraction_min_ordinary':.95,'load8_vs_normal2_min':2,
       'quiet_seconds_between_notifications':10,'max_gap_seconds':3,
       'short_extra_event_does_not_require_two_complete_windows':True,
       'scope':'Development engineering rules; closed stage 2 unchanged. No significance tests.'}
CORE_FILES=('cycle_profile.py','cycle_outputs.py','efficiency_baselines.py','immediate.py','detector.py','_reference_models.py')


def code_hashes():
    package=Path(__file__).resolve().parent
    return {name:sha(package/name) for name in (*CORE_FILES,'cycle_pipeline.py','cycle_collection.py')}


def read(path):return json.loads(Path(path).read_text(encoding='utf-8'))
def lines(path):
    with Path(path).open(encoding='utf-8') as stream:
        return [json.loads(s,parse_constant=lambda v:(_ for _ in ()).throw(ValueError('Nonfinite JSON.'))) for s in stream]
def write(path,value):
    with Path(path).open('x',encoding='utf-8') as stream:stream.write(encode(value))
def ratio(seq):return sum(seq)/len(seq) if seq else 0.
def median(seq):return statistics.median(seq) if seq else None
@contextmanager
def readonly(path):
    db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True);db.row_factory=sqlite3.Row
    try:yield db
    finally:db.close()


def verify_seal(folder,marker):
    folder=Path(folder).resolve();seal=read(folder/marker);names=set()
    for record in seal['files']:
        path=(folder/record['path']).resolve()
        if (path in names or not path.is_relative_to(folder) or not path.is_file() or
            path.stat().st_size!=record['bytes'] or sha(path)!=record['sha256']):
            raise ValueError('Sealed input changed: '+record['path'])
        names.add(path)
    return seal


def collection_info(collection):
    collection=Path(collection).resolve();seal=verify_seal(collection,'collection_complete.json')
    snapshot=collection/'application_snapshot.sqlite3'
    with snapshot.open('rb') as stream:header=stream.read(100)
    if header[:16]!=b'SQLite format 3\x00' or header[18:20]!=b'\x01\x01' or any(Path(str(snapshot)+suffix).exists() for suffix in ('-wal','-shm','-journal')):
        raise ValueError('Sealed database must have no unsealed WAL/journal changes.')
    run=seal['run']
    required={'application_snapshot.sqlite3','collection_status.json','schedule.json','schedule_log.jsonl',
              'testbench/response_receipts.jsonl','testbench/collector_closed.json',f'testbench/{run}-cycle-anchor.json',
              'source_manifest.json','code.zip'}
    if seal.get('profile')!=PROFILE or {r['path'] for r in seal['files']}!=required:
        raise ValueError('Wrong collection seal or missing files.')
    status=read(collection/'collection_status.json');closed=read(collection/'testbench/collector_closed.json')
    if status.get('status')!='collection_complete_awaiting_measurement_audit' or status.get('run')!=run or status.get('input_profile')!=PROFILE:
        raise ValueError('Incomplete collection.')
    if closed.get('run')!=run or not closed.get('own_child_exited') or not closed.get('collector_exited'):
        raise ValueError('Collection is not closed.')
    source=read(collection/'source_manifest.json');listed={r['path']:r for r in source['files']}
    with zipfile.ZipFile(collection/'code.zip') as archive:
        if len(listed)!=len(source['files']) or set(archive.namelist())!=set(listed) or len(archive.infolist())!=len(listed):
            raise ValueError('Source archive list differs.')
        import hashlib
        for name,record in listed.items():
            body=archive.read(name)
            if len(body)!=record['bytes'] or hashlib.sha256(body).hexdigest()!=record['sha256']:
                raise ValueError('Source archive content differs.')
    if any(listed.get(name,{}).get('sha256')!=sha(Path(__file__).resolve().parent/name) for name in CORE_FILES):
        raise ValueError('Scoring core differs from collection snapshot; use its fixed version.')
    return seal


def export(collection,output):
    """Reads run metadata and raw counters only. No outcomes or stored decisions."""
    collection=Path(collection).resolve();output=Path(output).resolve();seal=collection_info(collection)
    output.mkdir(parents=True,exist_ok=False)
    write(output/'processing_protocol.json',RULES)
    run=seal['run'];anchor=read(collection/f'testbench/{run}-cycle-anchor.json')
    if anchor.get('profile')!=PROFILE or anchor.get('role')!='raw_anchor_not_a_training_row':raise ValueError('Wrong anchor.')
    previous=project(anchor['counters']);count=0;last_arrival=last_mono=None
    with readonly(collection/'application_snapshot.sqlite3') as db:
        info=db.execute('SELECT status,error,config FROM runs WHERE id=?',(run,)).fetchone()
        if info is None or info['status']!='stopped' or info['error']:raise ValueError('Run did not stop cleanly.')
        config=json.loads(info['config'])
        if config.get('input_profile')!=PROFILE or config.get('features')!=list(FEATURES):raise ValueError('Wrong input profile.')
        with (output/'measurements.jsonl').open('x',encoding='utf-8') as stream:
            stream.write(encode(previous)+'\n')
            for record in db.execute('SELECT ts,counters,detector_values FROM samples WHERE run=? ORDER BY id',(run,)):
                raw=json.loads(record['counters']);clean=project(raw);check_pair(previous,clean)
                arrival=raw.get('collector_received_at');mono=raw.get('collector_received_monotonic_ns')
                if (type(arrival) not in (int,float) or not math.isfinite(arrival) or arrival<clean['timestamp'] or
                    type(mono) is not int or mono<=0 or (last_arrival is not None and arrival<=last_arrival) or
                    (last_mono is not None and mono<=last_mono) or record['ts']!=clean['timestamp']):
                    raise ValueError('Invalid acquisition times.')
                expected=detector_values(previous,clean)
                if json.loads(record['detector_values'])!=expected:raise ValueError('Stored input values differ from raw counters.')
                # Metadata below is NEVER a detector input; it is for the measurement audit.
                fields={k:raw[k] for k in ('client_target_rps','client_schedule_attempts','client_submitted_requests','client_skipped_slots')}
                stream.write(encode(dict(clean,collector_received_at=arrival,collector_received_monotonic_ns=mono,**fields))+'\n')
                count+=1;previous=clean;last_arrival=arrival;last_mono=mono
    if count<=896:raise ValueError('Incomplete initial learning or no monitoring data.')
    collection_info(collection)
    result={'profile':PROFILE,'run':run,'collection':str(collection),'collection_seal_sha256':sha(collection/'collection_complete.json'),
            'input_snapshots':count+1,'quality_evaluated':False,'outcomes_parsed':False,
            'processing_code_sha256':code_hashes(),
            'files':[dict(path=n,bytes=(output/n).stat().st_size,sha256=sha(output/n)) for n in ('processing_protocol.json','measurements.jsonl')]}
    write(output/'export_complete.json',result);return result


def verify_export(output):
    output=Path(output).resolve();seal=verify_seal(output,'export_complete.json')
    if (seal.get('profile')!=PROFILE or {r['path'] for r in seal['files']}!={'processing_protocol.json','measurements.jsonl'}
        or read(output/'processing_protocol.json')!=RULES or seal.get('processing_code_sha256')!=code_hashes()):raise ValueError('Wrong export/protocol/code version.')
    collection=Path(seal['collection']);collection_info(collection)
    if sha(collection/'collection_complete.json')!=seal['collection_seal_sha256']:raise ValueError('Collection version changed.')
    return seal


def schedule_segments(collection,last_timestamp):
    log=lines(collection/'schedule_log.jsonl');order=read(collection/'schedule.json')['order']
    post=[r for r in log if r.get('post_elapsed_seconds') is not None]
    training=[r for r in log if r.get('post_elapsed_seconds') is None]
    groups=[]
    for row in training:
        group=row['samples']//32;action=row['action']
        if (not 0<=row['samples']<896 or action['kind']!='load' or action['value']!=(2,4,8)[group%3] or action['seconds']!=120):
            raise ValueError('Initial ordinary load cycle differs from protocol.')
        if not groups or group!=groups[-1]:groups.append(group)
    if groups!=list(range(28)) or training[0]['samples'] not in (0,1):raise ValueError('Initial load cycle incomplete.')
    expected=[asdict(a) for a in post_schedule(order)]
    if [r['action'] for r in post]!=expected:raise ValueError('Post schedule incomplete or changed.')
    for row in post:
        if not 0<=row['post_elapsed_seconds']-row['action']['at']<=1:raise ValueError('Late scheduled action.')
    if not post or abs(last_timestamp-post[-1]['requested_wall_time'])>3:raise ValueError('Incomplete final observations.')
    commands=[];previous_time=-math.inf
    for row in log:
        t=row['completed_wall_time'];action=row['action'];kind=action['kind']
        if not previous_time<=row['requested_wall_time']<=t or t-row['requested_wall_time']>3:raise ValueError('Invalid command time.')
        previous_time=t
        if kind=='stop':break
        mode='extra_cpu' if kind=='extra_cpu' else 'normal'
        rate=action['value'] if kind=='load' else 2
        category=('training' if row['post_elapsed_seconds'] is None else
                  'increased_load' if rate>2 else 'baseline' if action['at']==0 else 'return_to_normal')
        if commands and commands[-1]['mode']==mode and commands[-1]['rate']==rate and commands[-1]['category']==category:
            continue
        commands.append(dict(start=t,requested=row['requested_wall_time'],mode=mode,rate=rate,category=category))
    for i,row in enumerate(commands):row['end']=commands[i+1]['requested'] if i+1<len(commands) else last_timestamp
    return commands,post


def load_outcomes(collection,run):
    with readonly(collection/'application_snapshot.sqlite3') as db:
        return [dict(r) for r in db.execute('SELECT r.*,w.cpu_ms,w.wall_ms,w.iterations FROM requests r LEFT JOIN request_work w ON w.request_id=r.id WHERE r.run=? ORDER BY r.sent,r.id',(run,))]


def audit(output):
    output=Path(output).resolve();exported=verify_export(output);collection=Path(exported['collection'])
    rows=lines(output/'measurements.jsonl');segments,post=schedule_segments(collection,rows[-1]['timestamp'])
    # Outcome reads here check only execution/success, never comparative detection.
    requests=load_outcomes(collection,exported['run']);receipts=lines(collection/'testbench/response_receipts.jsonl')
    closed=read(collection/'testbench/collector_closed.json');previous_counts=None
    for raw in rows[1:]:
        counts=[raw[k] for k in ('client_schedule_attempts','client_submitted_requests','client_skipped_slots')]
        if any(type(n) is not int or n<0 for n in counts) or counts[0]!=counts[1]+counts[2] or (previous_counts and any(a<b for a,b in zip(counts,previous_counts))):
            raise ValueError('Invalid client counters.')
        if raw['client_target_rps'] not in (2,4,8):raise ValueError('Unexpected target rate.')
        previous_counts=counts
    request_keys={(r['sent'],r['received']) for r in requests}
    receipt_keys={(r['sent'],r['received']) for r in receipts}
    receipt_by_key={(r['sent'],r['received']):r for r in receipts}
    same_response=(bool(receipts) and len({r.get('body_sha256') for r in receipts})==1 and
                   all(isinstance(r.get('body_sha256'),str) and len(r['body_sha256'])==64 and r.get('run')==exported['run'] for r in receipts))
    summaries=[];screens=[];extra_segments=[]
    for segment in segments:
        if segment['mode']=='extra_cpu':
            extra_segments.append(segment);continue
        interior=[r for r in rows[1:] if segment['start']+3<=r['timestamp']<=segment['end']-2]
        window=CycleWindow();candidates=[];eligible=[];disjoint=[];last_end=-math.inf
        for raw in interior:
            result=window.observe(raw)
            if raw['timestamp']<interior[0]['timestamp']+10:continue
            candidates.append(result)
            if result['valid'] and result['ratio_eligible'] and result['resource_cycles']>0:
                eligible.append(result);screens.append(result['read_alignment_full_width_fraction']<=.05)
                if result['window_start']>=last_end:disjoint.append(result);last_end=result['timestamp']
        selected=[r for r in requests if segment['start']+3<=r['sent'] and r['received']<=segment['end']-2]
        summaries.append(dict(**segment,candidate_count=len(candidates),eligible_count=len(eligible),
            eligible_fraction=len(eligible)/len(candidates) if candidates else 0,
            positive_fraction=ratio([r['valid'] and r.get('resource_cycles',0)>0 for r in candidates]),
            disjoint_count=len(disjoint),actual_rate=median([r['completed']/r['duration_seconds'] for r in disjoint]),
            actual_rate_count=len(disjoint),normal_extra_absent=bool(selected) and all(r['served_mode']=='normal' and r['iterations']==0 for r in selected),
            max_read_width_fraction=max((r['read_alignment_full_width_fraction'] for r in eligible),default=None)))
    rates={rate:[s['actual_rate'] for s in summaries if s['rate']==rate and s['actual_rate'] is not None] for rate in (2,4,8)}
    growth=median(rates[8])/median(rates[2]) if rates[8] and rates[2] else None
    extra=[]
    for segment in extra_segments:
        actual=[r for r in requests if r['served_mode']=='extra_cpu' and r['iterations'] and r['iterations']>0 and r['sent']<=segment['end'] and r['received']>=segment['requested']]
        extra.append(dict(requests=len(actual),executed=bool(actual),short_event_complete_windows_not_required=True))
    gates={'ordinary_coverage':bool(summaries) and all(s['eligible_fraction']>=.9 for s in summaries),
           'ordinary_positive_cycles':all(s['positive_fraction']>=.9 for s in summaries),
           'two_disjoint_windows_each_ordinary_segment':all(s['disjoint_count']>=2 for s in summaries),
           'read_alignment':ratio(screens)>=.95,'normal_extra_absent':all(s['normal_extra_absent'] for s in summaries),
           'requests_successful':bool(requests) and all(r['status']==200 and not r['error'] and r['received']>=r['sent'] for r in requests),
           'receipt_journal_match':len(requests)==len(receipts)==len(request_keys)==len(receipt_keys) and request_keys==receipt_keys and
               all(receipt_by_key[(r['sent'],r['received'])].get('status')==r['status'] and
                   receipt_by_key[(r['sent'],r['received'])].get('error')==r['error'] for r in requests),
           'useful_response_identical':same_response,
           'no_skips_and_complete_request_accounting':closed['skipped']==0 and closed['scheduled']==closed['submitted']==len(requests),
           'actual_load_growth':growth is not None and growth>=2,
           'two_executed_extra_events':len(extra)==2 and all(e['executed'] for e in extra),
           'request_work_consistent':all(r['served_mode'] in ('normal','extra_cpu') and type(r['iterations']) is int and
               (r['iterations']>0 if r['served_mode']=='extra_cpu' else r['iterations']==0) for r in requests),
           '896_rows_then_complete_monitoring':post[0]['samples']==896 and len(rows)>897}
    report=dict(version=VERSION,profile=PROFILE,admitted=all(gates.values()),gates=gates,segments=summaries,
                extra_execution=extra,load_growth=growth,ordinary_width_pass_fraction=ratio(screens),
                quality_evaluated=False,precision_certified=False,export_seal_sha256=sha(output/'export_complete.json'),
                collection_seal_sha256=exported['collection_seal_sha256'],protocol_sha256=sha(output/'processing_protocol.json'))
    verify_export(output);write(output/'measurement_audit.json',report);return report


def admitted(output):
    output=Path(output).resolve();exported=verify_export(output);report=read(output/'measurement_audit.json')
    if (not report.get('admitted') or report.get('export_seal_sha256')!=sha(output/'export_complete.json') or
        report.get('protocol_sha256')!=sha(output/'processing_protocol.json') or report.get('version')!=VERSION):
        raise ValueError('Measurement admission is missing, failed, or belongs to different inputs.')
    return exported


def score(output):
    output=Path(output).resolve();admitted(output)
    barrier=save_outputs(lines(output/'measurements.jsonl'),output/'four_outputs')
    write(output/'scoring_complete.json',dict(profile=PROFILE,export_seal_sha256=sha(output/'export_complete.json'),
        measurement_audit_sha256=sha(output/'measurement_audit.json'),four_outputs_barrier_sha256=sha(output/'four_outputs/four_outputs_complete.json'),
        processing_code_sha256=sha(Path(__file__)),protocol_sha256=sha(output/'processing_protocol.json'),post_warmup_rows=barrier['post_warmup_rows']))
    return barrier


def episodes(records):
    """Unknown/gaps never count as observed recovery. No extrapolated tail."""
    found=[];quiet_since=None;previous=None;current=None;alarm_seconds=0.
    for row in records:
        t=row['timestamp'];alarm=row['alarm']
        if previous is not None:
            dt=t-previous['timestamp']
            if not 0<dt<=3:quiet_since=None
            elif previous['alarm'] is True:alarm_seconds+=dt
        if alarm is True:
            if current is None or (quiet_since is not None and t-quiet_since>=10):
                current={'start':t,'available_at':row['available_at']};found.append(current)
            quiet_since=None
        elif alarm is False:
            if quiet_since is None:quiet_since=t
        else:quiet_since=None
        previous=row
    return found,alarm_seconds


def summarize_method(records,events,segments):
    notifications,duration=episodes(records);event_results=[]
    for event in events:
        inside=[n for n in notifications if event['start']<=n['start']<=event['end']]
        timely=[n for n in inside if n['available_at']<=event['end']]
        event_results.append(dict(event,detected_before_end=bool(timely),
            delay_seconds=min(n['available_at'] for n in timely)-event['start'] if timely else None,
            late_availability_count=sum(n['available_at']>event['end'] for n in inside)))
    ordinary=[]
    for segment in segments:
        if segment['category']=='training' or segment['mode']!='normal':continue
        alerts=[n for n in notifications if segment['start']<=n['start']<segment['end'] and
                not any(e['start']<=n['start']<=e['end'] for e in events)]
        ordinary.append(dict(category=segment['category'],rate=segment['rate'],start=segment['start'],end=segment['end'],notifications=len(alerts)))
    outside=[n for n in notifications if not any(e['start']<=n['start']<=e['end'] for e in events)]
    unclassified=[n for n in outside if not any(s['start']<=n['start']<s['end'] and s['mode']=='normal' and s['category']!='training' for s in segments)]
    return dict(events=event_results,ordinary_segments=ordinary,notifications=notifications,
                notifications_outside_actual_event_envelopes=len(outside),unclassified_transition_notifications=unclassified,
                observed_alarm_seconds=duration,unknown_decision_rows=sum(r['alarm'] is None for r in records))


def quality(output):
    output=Path(output).resolve();exported=admitted(output);barrier=verify_outputs(output/'four_outputs')
    binding=read(output/'scoring_complete.json')
    expected={'export_seal_sha256':sha(output/'export_complete.json'),'measurement_audit_sha256':sha(output/'measurement_audit.json'),
              'four_outputs_barrier_sha256':sha(output/'four_outputs/four_outputs_complete.json'),
              'processing_code_sha256':sha(Path(__file__)),'protocol_sha256':sha(output/'processing_protocol.json')}
    if any(binding.get(k)!=v for k,v in expected.items()):raise ValueError('Scoring version/binding changed.')
    rows=lines(output/'measurements.jsonl');traces=lines(output/'four_outputs/comparison_trace.jsonl')
    expected_timestamps=[r['timestamp'] for r in rows[897:]]
    if [t['timestamp'] for t in traces]!=expected_timestamps:raise ValueError('Trace does not cover the exact export.')
    projected=lines(output/'four_outputs/projected_measurements.jsonl')
    if len(projected)!=len(rows) or any(project(a)!=project(b) or a.get('collector_received_at')!=b.get('collector_received_at') for a,b in zip(rows,projected)):
        raise ValueError('Scored inputs differ from admitted export.')
    collection=Path(exported['collection']);segments,post=schedule_segments(collection,rows[-1]['timestamp'])
    # FIRST quality outcome read is below BOTH verified barriers.
    requests=load_outcomes(collection,exported['run']);events=[];used=set()
    for segment in segments:
        if segment['mode']!='extra_cpu':continue
        selected=[r for r in requests if r['served_mode']=='extra_cpu' and r['iterations'] and r['iterations']>0 and
                  r['sent']<=segment['end'] and r['received']>=segment['requested']]
        if not selected or any(r['id'] in used for r in selected):raise ValueError('Missing or ambiguous actual event.')
        used.update(r['id'] for r in selected)
        events.append(dict(start=min(r['sent'] for r in selected),end=max(r['received'] for r in selected),affected_requests=len(selected)))
    if len(events)!=2:raise ValueError('Expected two actual events.')
    methods={}
    for name in NAMES:
        saved=lines(output/'four_outputs'/(name+'.jsonl'))
        if [r['timestamp'] for r in saved]!=expected_timestamps:raise ValueError('Method timestamps differ.')
        records=[];previous_available=-math.inf;costs=[];extrapolated=0
        for raw,trace,item in zip(rows[897:],traces,saved):
            original=trace['decisions'][name]
            if any(item.get(k)!=v for k,v in original.items()):raise ValueError('Method and trace decisions differ.')
            common=trace['common_input_preparation_seconds']
            own=(original['own_compute_seconds'] if name=='ipca' else
                 original['own_score_seconds']+trace['simple_update_seconds'][name])
            window=0. if name=='ipca' else trace['common_window_preparation_seconds']
            elapsed=common+window+own
            if not math.isfinite(elapsed) or elapsed<0:raise ValueError('Invalid measured computation cost.')
            available=max(raw['collector_received_at'],previous_available)+elapsed;previous_available=available
            records.append(dict(timestamp=item['timestamp'],alarm=item['alarm'],available_at=available))
            costs.append(dict(shared_input=common,shared_window=window,own=own,total=elapsed))
            extrapolated+=bool(trace['expected_cycles_extrapolation']) if name=='expected_cycles' else 0
        methods[name]=summarize_method(records,events,segments)
        methods[name]['compute_seconds']={key:sum(c[key] for c in costs) for key in costs[0]}
        methods[name]['expected_relation_extrapolation_rows']=extrapolated
    result=dict(version=VERSION,profile=PROFILE,methods=methods,events=events,post_warmup_rows=barrier['post_warmup_rows'],
        scoring_binding_sha256=sha(output/'scoring_complete.json'),quality_evaluated=True,
        timing_scope='Approximate offline availability: measured input preparation + relevant window preparation + own computation/update; per-method backlog. Not service/browser delivery; unmeasured orchestration and warmup backlog excluded.',
        alarm_duration_scope='Left-held alarm over observed adjacent intervals up to 3 seconds; no tail extrapolation.',
        memory_scope='Application RSS is an input, not memory consumption of four detectors; detector memory not measured.',
        event_scope='Envelope from first sent to last received actually affected request, not exact CPU execution interval.',
        inference_scope='Descriptive development run, no significance tests, no proof of novelty or superiority.')
    verify_export(output);verify_outputs(output/'four_outputs');write(output/'quality.json',result);return result


def process(collection,output):
    export(collection,output)
    result=audit(output)
    if not result['admitted']:return {'status':'stopped_at_measurement_gate','quality_evaluated':False}
    score(output);quality(output)
    return {'status':'processing_complete','quality_evaluated':True}


def main():
    parser=argparse.ArgumentParser(description='Только разбор закрытой записи; сбор приложения не запускается.')
    parser.add_argument('--stage',choices=('process','export','audit','score','quality'),default='process')
    parser.add_argument('--collection',type=Path);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.stage in ('process','export'):
        if args.collection is None:parser.error('--collection required')
        result=(process if args.stage=='process' else export)(args.collection,args.output)
    else:result={'audit':audit,'score':score,'quality':quality}[args.stage](args.output)
    print(json.dumps({'stage':args.stage,'status':result.get('status','saved'),'quality_evaluated':result.get('quality_evaluated',False)},ensure_ascii=False))


if __name__=='__main__':main()
