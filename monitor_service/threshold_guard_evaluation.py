"""Eight-output evaluation; new-series outcomes require ALL nine admissions.

Pure summary helpers also support explicit old-record development/software tests.
No collection, fitting, tuning or output generation is performed here.
"""
import math
from pathlib import Path
from . import cycle_pipeline as legacy
from .cycle_profile import project
from .threshold_guard import verify_outputs
from .threshold_guard_protocol import load_protocol,NAMES,SPEC,sha
from . import threshold_guard_measurements as measurements


def ordinary_duration(records,segments,events):
    allowed=[]
    for s in segments:
        if s['category']=='training' or s['mode']!='normal':continue
        pieces=[(s['start'],s['end'])]
        for e in events:
            next_pieces=[]
            for a,b in pieces:
                if e['end']<=a or e['start']>=b:next_pieces.append((a,b))
                else:
                    if a<e['start']:next_pieces.append((a,e['start']))
                    if e['end']<b:next_pieces.append((e['end'],b))
            pieces=next_pieces
        allowed.extend((a,b) for a,b in pieces if b>a)
    allowed.sort()
    if any(b>c for (a,b),(c,d) in zip(allowed,allowed[1:])):raise ValueError('Overlapping ordinary intervals.')
    total=0.
    for a,b in zip(records,records[1:]):
        if a['alarm'] is not True or not 0<b['timestamp']-a['timestamp']<=3:continue
        total+=sum(max(0.,min(b['timestamp'],end)-max(a['timestamp'],start)) for start,end in allowed)
    return total


def summarize(records,events,segments):
    previous=-math.inf
    for row in records:
        if (not math.isfinite(row['timestamp']) or row['timestamp']<=previous or
                row['alarm'] is not None and type(row['alarm']) is not bool or
                not math.isfinite(row['available_at']) or row['available_at']<row['timestamp']):
            raise ValueError('Invalid decisions/availability chronology.')
        previous=row['timestamp']
    result=legacy.summarize_method(records,events,segments)
    result['ordinary_notification_count']=sum(s['notifications'] for s in result['ordinary_segments'])
    result['ordinary_alarm_seconds']=ordinary_duration(records,segments,events)
    result['point_alarm_count']=sum(r['alarm'] is True for r in records)
    return result


def summarize_saved(output,events,segments):
    output=Path(output);verify_outputs(output);methods={}
    for name in NAMES:
        records=[];previous=-math.inf
        for d in legacy.lines(output/(name+'.jsonl')):
            cost=d['availability_cost_seconds']
            if not math.isfinite(cost) or cost<0:raise ValueError('Invalid composite cost.')
            available=max(d['collector_received_at'],previous)+cost;previous=available
            records.append(dict(timestamp=d['timestamp'],alarm=d['alarm'],available_at=available))
        methods[name]=summarize(records,events,segments)
    return methods


def verify_series(entries):
    cfg=load_protocol();expected=cfg['collection']['planned_order']
    if len(entries)!=len(expected):raise ValueError('All nine records are required before any quality read.')
    bindings=[];runs=set();snapshots=set()
    for entry,scenario in zip(entries,expected):
        folder=Path(entry['processing']).resolve();scores=Path(entry['outputs']).resolve()
        exported,audit=measurements.admitted(folder);barrier=verify_outputs(scores)
        if audit['scenario']!=scenario:raise ValueError('Predeclared scenario order differs.')
        if audit['run'] in runs or exported['collection_seal_sha256'] in snapshots:raise ValueError('A record cannot be reused as another repeat.')
        runs.add(audit['run']);snapshots.add(exported['collection_seal_sha256'])
        rows=legacy.lines(folder/'measurements.jsonl');projected=legacy.lines(scores/'projected_measurements.jsonl')
        if len(rows)!=len(projected) or barrier['input_rows']!=len(rows):raise ValueError('Output input coverage differs.')
        for a,b in zip(rows,projected):
            if project(a)!=project(b) or a.get('collector_received_at')!=b.get('collector_received_at'):
                raise ValueError('Scored inputs differ from admitted inputs.')
        bindings.append(dict(processing=str(folder),outputs=str(scores),scenario=scenario,run=audit['run'],
            export_sha256=sha(folder/'export_complete.json'),admission_sha256=sha(folder/'guard_measurement_admission.json'),
            output_barrier_sha256=sha(scores/'eight_outputs_complete.json')))
    return dict(protocol_sha256=sha(SPEC),evaluation_code_sha256=sha(Path(__file__)),records=bindings,quality_evaluated=False)


def build_series_gate(entries,path):
    gate=verify_series(entries);legacy.write(Path(path),gate);return gate


def actual_events(parts,requests,scenario):
    events=[];used=set()
    for segment in parts:
        if segment['mode']!='extra_cpu':continue
        selected=[r for r in requests if r['served_mode']=='extra_cpu' and r['iterations'] and r['iterations']>0 and r['sent']<=segment['end'] and r['received']>=segment['requested']]
        if not selected or any(r['id'] in used for r in selected):raise ValueError('Missing or overlapping actual event.')
        used.update(r['id'] for r in selected)
        events.append(dict(start=min(r['sent'] for r in selected),end=max(r['received'] for r in selected),affected_requests=len(selected)))
    if len(events)!=(0 if scenario=='C' else 2):raise ValueError('Wrong actual event count.')
    return events


def screening(records,prefix):
    limited=prefix+'_limited';updated=prefix+'_updated';fixed=prefix+'_fixed'
    def no_event_loss(a,b):
        return all(not eb['detected_before_end'] or ea['detected_before_end'] for ea,eb in zip(a['events'],b['events']))
    def no_ordinary_worse(a,b):
        return a['ordinary_notification_count']<=b['ordinary_notification_count'] and a['ordinary_alarm_seconds']<=b['ordinary_alarm_seconds']+1e-6
    a_records=[r for r in records if r['scenario']=='A']
    gain=sum(r['methods'][limited]['events'][1]['detected_before_end'] and not r['methods'][updated]['events'][1]['detected_before_end'] for r in a_records)
    updated_ok=gain>=2 and all(no_event_loss(r['methods'][limited],r['methods'][updated]) and no_ordinary_worse(r['methods'][limited],r['methods'][updated]) for r in records)
    fixed_no_worse=all(no_event_loss(r['methods'][limited],r['methods'][fixed]) and no_ordinary_worse(r['methods'][limited],r['methods'][fixed]) for r in records)
    extra_events=sum(sum(e['detected_before_end'] for e in r['methods'][limited]['events'])-sum(e['detected_before_end'] for e in r['methods'][fixed]['events']) for r in records)
    ordinary_gain=sum(r['methods'][fixed]['ordinary_notification_count']-r['methods'][limited]['ordinary_notification_count'] for r in records)
    seconds_gain=sum(r['methods'][fixed]['ordinary_alarm_seconds']-r['methods'][limited]['ordinary_alarm_seconds'] for r in records)
    return dict(passes=updated_ok and fixed_no_worse and (extra_events>=1 or ordinary_gain>=1 or seconds_gain>=10),
        extra_A_short_detections=gain,updated_comparison_passes=updated_ok,fixed_nonworsening=fixed_no_worse,
        extra_events_over_fixed=extra_events,fewer_ordinary_notifications_than_fixed=ordinary_gain,fewer_ordinary_seconds_than_fixed=seconds_gain)


def evaluate_series(entries,gate_path,output):
    # Verification of ALL entries precedes the FIRST quality outcome read.
    expected=verify_series(entries);saved=legacy.read(gate_path)
    if saved!=expected:raise ValueError('Nine-record gate changed or belongs to other files.')
    records=[]
    for entry,binding in zip(entries,saved['records']):
        folder=Path(entry['processing']);exported,_=measurements.admitted(folder)
        rows=legacy.lines(folder/'measurements.jsonl')
        scenario,parts,_=measurements.segments(Path(exported['collection']),rows[-1]['timestamp'])
        requests=legacy.load_outcomes(Path(exported['collection']),exported['run'])
        events=actual_events(parts,requests,scenario)
        records.append(dict(scenario=scenario,run=exported['run'],events=events,methods=summarize_saved(entry['outputs'],events,parts)))
    result=dict(protocol_sha256=sha(SPEC),series_gate_sha256=sha(gate_path),records=records,
        primary=screening(records,'ratio'),secondary=screening(records,'expected'),quality_evaluated=True,
        scope='Predeclared engineering screen, not statistical proof or IPCA improvement.')
    if verify_series(entries)!=saved:raise ValueError('Inputs changed during evaluation.')
    legacy.write(Path(output),result);return result
