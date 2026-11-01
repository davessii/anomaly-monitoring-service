"""Scenario-aware MEASUREMENT admission; never computes detection quality.

Reuses only the legacy whitelist export and raw helpers, not legacy process.
New schedule A/B/C and zero-event C are validated explicitly.
"""
from dataclasses import asdict
import math
from pathlib import Path
from . import cycle_pipeline as raw
from .cycle_profile import CycleWindow
from .threshold_guard_protocol import SPEC,sha,load_protocol
from .threshold_guard_schedule import actions


def segments(collection,last_timestamp):
    collection=Path(collection);scenario=raw.read(collection/'schedule.json')['scenario']
    expected=[asdict(a) for a in actions(scenario)]
    log=raw.lines(collection/'schedule_log.jsonl');training=[r for r in log if r.get('post_elapsed_seconds') is None]
    post=[r for r in log if r.get('post_elapsed_seconds') is not None];groups=[]
    for row in training:
        group=row['samples']//32;a=row['action']
        if not 0<=row['samples']<896 or a['kind']!='load' or a['value']!=(2,4,8)[group%3] or a['seconds']!=120:
            raise ValueError('Initial training load differs.')
        if not groups or groups[-1]!=group:groups.append(group)
    if groups!=list(range(28)) or training[0]['samples'] not in (0,1):raise ValueError('Training groups incomplete.')
    if [r['action'] for r in post]!=expected or post[0]['samples']!=896:
        raise ValueError('Scenario actions or 896-row transition differ.')
    if any(not 0<=r['post_elapsed_seconds']-r['action']['at']<=1 for r in post):raise ValueError('Late scenario command.')
    if abs(last_timestamp-post[-1]['requested_wall_time'])>3:raise ValueError('Final observations incomplete.')
    commands=[];previous=-math.inf
    for row in log:
        a=row['action'];t=row['completed_wall_time']
        if not previous<=row['requested_wall_time']<=t or t-row['requested_wall_time']>3:raise ValueError('Command time invalid.')
        previous=t
        if a['kind']=='stop':break
        mode='extra_cpu' if a['kind']=='extra_cpu' else 'normal';rate=a['value'] if a['kind']=='load' else 2
        category='training' if row.get('post_elapsed_seconds') is None else 'increased_load' if rate>2 else 'baseline' if a['at']==0 else 'return_to_normal'
        if commands and all(commands[-1][k]==v for k,v in dict(mode=mode,rate=rate,category=category).items()):continue
        commands.append(dict(start=t,requested=row['requested_wall_time'],mode=mode,rate=rate,category=category))
    for i,row in enumerate(commands):row['end']=commands[i+1]['requested'] if i+1<len(commands) else last_timestamp
    return scenario,commands,post


def checks(rows,requests,receipts,closed,parts,post,event_count):
    previous=None
    for row in rows[1:]:
        counts=[row[k] for k in ('client_schedule_attempts','client_submitted_requests','client_skipped_slots')]
        if any(type(n) is not int or n<0 for n in counts) or counts[0]!=counts[1]+counts[2] or (previous and any(a<b for a,b in zip(counts,previous))):
            raise ValueError('Client counter chronology invalid.')
        if row['client_target_rps'] not in (2,4,8):raise ValueError('Wrong target rate.')
        previous=counts
    keys={(r['sent'],r['received']) for r in requests};receipt_keys={(r['sent'],r['received']) for r in receipts}
    by_key={(r['sent'],r['received']):r for r in receipts};summaries=[];screens=[];extra=[];assigned=[]
    for part in parts:
        if part['mode']=='extra_cpu':
            selected=[r for r in requests if r['served_mode']=='extra_cpu' and r['iterations'] and r['iterations']>0 and r['sent']<=part['end'] and r['received']>=part['requested']]
            extra.append(dict(requests=len(selected),executed=bool(selected)))
            assigned.extend((r['sent'],r['received']) for r in selected);continue
        interior=[r for r in rows[1:] if part['start']+3<=r['timestamp']<=part['end']-2]
        window=CycleWindow();candidates=[];eligible=[];disjoint=[];last_end=-math.inf
        for row in interior:
            w=window.observe(row)
            if row['timestamp']<interior[0]['timestamp']+10:continue
            candidates.append(w)
            if w['valid'] and w['ratio_eligible'] and w['resource_cycles']>0:
                eligible.append(w);screens.append(w['read_alignment_full_width_fraction']<=.05)
                if w['window_start']>=last_end:disjoint.append(w);last_end=w['timestamp']
        selected=[r for r in requests if part['start']+3<=r['sent'] and r['received']<=part['end']-2]
        summaries.append(dict(**part,eligible_fraction=len(eligible)/len(candidates) if candidates else 0,
            positive_fraction=raw.ratio([w['valid'] and w.get('resource_cycles',0)>0 for w in candidates]),
            disjoint_count=len(disjoint),actual_rate=raw.median([w['completed']/w['duration_seconds'] for w in disjoint]),
            normal_extra_absent=bool(selected) and all(r['served_mode']=='normal' and r['iterations']==0 for r in selected)))
    rates={n:[s['actual_rate'] for s in summaries if s['rate']==n and s['actual_rate'] is not None] for n in (2,8)}
    growth=raw.median(rates[8])/raw.median(rates[2]) if rates[8] and rates[2] else None
    same_response=bool(receipts) and len({r.get('body_sha256') for r in receipts})==1 and all(isinstance(r.get('body_sha256'),str) and len(r['body_sha256'])==64 and r.get('run')==closed['run'] for r in receipts)
    gates=dict(ordinary_coverage=bool(summaries) and all(s['eligible_fraction']>=.9 for s in summaries),
        ordinary_positive_cycles=all(s['positive_fraction']>=.9 for s in summaries),
        two_disjoint_windows_each_ordinary_segment=all(s['disjoint_count']>=2 for s in summaries),
        read_alignment=raw.ratio(screens)>=.95,normal_extra_absent=all(s['normal_extra_absent'] for s in summaries),
        requests_successful=bool(requests) and all(r['status']==200 and not r['error'] and r['received']>=r['sent'] for r in requests),
        receipt_journal_match=len(requests)==len(receipts)==len(keys)==len(receipt_keys) and keys==receipt_keys and all(by_key[(r['sent'],r['received'])].get('status')==r['status'] and by_key[(r['sent'],r['received'])].get('error')==r['error'] for r in requests),
        useful_response_identical=same_response,
        no_skips_and_complete_request_accounting=closed['skipped']==0 and closed['scheduled']==closed['submitted']==len(requests),
        actual_load_growth=growth is not None and growth>=2,
        planned_extra_events_executed=len(extra)==event_count and all(e['executed'] for e in extra),
        request_work_consistent=all(r['served_mode'] in ('normal','extra_cpu') and type(r['iterations']) is int and (r['iterations']>0 if r['served_mode']=='extra_cpu' else r['iterations']==0) for r in requests),
        complete_monitoring=post[0]['samples']==896 and len(rows)>897)
    actual_extra={(r['sent'],r['received']) for r in requests if r['served_mode']=='extra_cpu' and r['iterations'] and r['iterations']>0}
    gates['all_extra_work_in_planned_events']=set(assigned)==actual_extra and len(assigned)==len(set(assigned))
    if event_count==0:gates['normal_only_no_extra_requests']=all(r['served_mode']=='normal' and r['iterations']==0 for r in requests)
    gates['writer_and_client_closed']=(closed.get('all_submitted_results_persisted') is True and closed.get('persistence',{}).get('complete') is True and closed.get('http_client',{}).get('accounting_complete') is True)
    return dict(admitted=all(gates.values()),gates=gates,segments=summaries,extra_execution=extra,load_growth=growth)


def export(collection,output):
    # Whitelist export does not assume two events. Never call raw.process here.
    return raw.export(collection,output)


def audit(output):
    load_protocol();output=Path(output);exported=raw.verify_export(output);collection=Path(exported['collection'])
    rows=raw.lines(output/'measurements.jsonl');scenario,parts,post=segments(collection,rows[-1]['timestamp'])
    requests=raw.load_outcomes(collection,exported['run']) # technical execution/success only
    report=checks(rows,requests,raw.lines(collection/'testbench/response_receipts.jsonl'),
                  raw.read(collection/'testbench/collector_closed.json'),parts,post,0 if scenario=='C' else 2)
    from .threshold_guard import source_hashes
    report.update(scenario=scenario,run=exported['run'],quality_evaluated=False,source_sha256=source_hashes(),
        protocol_sha256=sha(SPEC),export_sha256=sha(output/'export_complete.json'),
        implementation_sha256=sha(Path(__file__)))
    raw.verify_export(output);raw.write(output/'guard_measurement_admission.json',report);return report


def admitted(output):
    from .threshold_guard import source_hashes
    output=Path(output);exported=raw.verify_export(output);report=raw.read(output/'guard_measurement_admission.json')
    if (not report['admitted'] or not all(report['gates'].values()) or report['protocol_sha256']!=sha(SPEC)
            or report['export_sha256']!=sha(output/'export_complete.json') or report['implementation_sha256']!=sha(Path(__file__))
            or report.get('source_sha256')!=source_hashes()):
        raise ValueError('Missing, failed or changed scenario measurement admission.')
    return exported,report
