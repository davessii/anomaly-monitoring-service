"""Separate, descriptive application status. Never modifies detector decisions.

Incident views are rebuilt from recorded outcomes/decisions under the displayed
current service policy. They are not research event labels or scientific metrics.
"""
from datetime import datetime
from pathlib import Path
import json, math, os, threading

RECOVERY_SECONDS=10
FRESHNESS_SECONDS=10

class ServicePolicy:
    def __init__(self,path):
        self.path=Path(path);self.lock=threading.RLock()
        self.limit=None
        if self.path.exists():self.limit=self.validate(json.loads(self.path.read_text(encoding='utf-8')))
    @staticmethod
    def validate(data):
        if not isinstance(data,dict) or set(data)!={'latency_limit_ms'}:
            raise ValueError('Нужна только настройка latency_limit_ms; null отключает проверку времени.')
        value=data['latency_limit_ms']
        if value is not None and (type(value) not in (int,float) or not 0<value<=600000 or not math.isfinite(value)):
            raise ValueError('Допустимое время: положительное число до 600000 мс или пустое значение.')
        return value
    def get(self):
        with self.lock:
            return {'latency_limit_ms':self.limit,'recovery_seconds':RECOVERY_SECONDS,
                    'freshness_seconds':FRESHNESS_SECONDS,'scope':'current_settings_view'}
    def set(self,data):
        value=self.validate(data)
        with self.lock:
            self.path.parent.mkdir(parents=True,exist_ok=True)
            temporary=self.path.with_suffix('.tmp')
            temporary.write_text(json.dumps({'latency_limit_ms':value},allow_nan=False),encoding='utf-8')
            os.replace(temporary,self.path)
            self.limit=value
            return self.get()

def response_cause(row,limit):
    if row.get('error') is not None or row['status'] is None:return 'no_response'
    if not 200<=row['status']<400:return 'http_error'
    if limit is not None and row['duration_ms']>limit:return 'slow_response'
    return None

def group_events(events,ended=None,max_gap=5):
    """Resolve only after explicit normal observations spanning >=10 seconds.

    Missing records never prove recovery. Ending a run is not recovery. Multiple
    different bad outcomes stay in one episode until confirmed normal operation.
    """
    incidents=[];current=None;good_start=None;last_good=None
    for event in sorted(events,key=lambda e:e['time']):
        t=event['time'];causes=event['causes']
        if causes:
            if current is None:
                current={'started_at':t,'last_signal_at':t,'resolved_at':None,'status':'active',
                         'signals':0,'causes':{},'normal_since':None}
                incidents.append(current)
            current['last_signal_at']=t;current['signals']+=event.get('count',1)
            for cause in causes:current['causes'][cause]=current['causes'].get(cause,0)+event.get('count',1)
            current['status']='active';current['normal_since']=None
            good_start=last_good=None
        elif current is not None:
            if good_start is None or t-last_good>max_gap:good_start=t
            last_good=t;current['normal_since']=good_start;current['status']='recovering'
            if t-good_start>=RECOVERY_SECONDS:
                current.update(status='resolved',resolved_at=t)
                current=None;good_start=last_good=None
    if current is not None and ended is not None:
        current.update(status='observation_ended',observation_ended_at=ended)
    return incidents

def model_events(observations):
    # Batch16 decisions share one issuance moment; individual decisions keep
    # their original values. Grouping does not add or suppress a model alarm.
    groups={}
    for row in observations:
        if not row['result']:continue
        v=json.loads(row['result']) if isinstance(row['result'],str) else row['result']
        key=(v['decision_source_time'],v['emitted_at'])
        if key not in groups:
            groups[key]={'time':datetime.fromisoformat(v['emitted_at']).timestamp(),'causes':[], 'count':0}
        if v['alarm']:
            groups[key]['causes']=['unusual_metrics'];groups[key]['count']+=1
    return sorted(groups.values(),key=lambda e:e['time'])

def build_status(run,requests,observations,policy,now,delivery_rows=1):
    limit=policy['latency_limit_ms'];ended=run['ended']
    observed_until=ended if ended is not None else now
    rows=sorted((r for r in requests if r['received']<=observed_until),key=lambda r:(r['received'],r.get('id',0)))
    recent=[r for r in rows if observed_until-FRESHNESS_SECONDS<=r['received']<=observed_until]
    causes=[response_cause(r,limit) for r in recent]
    counts={name:causes.count(name) for name in ('no_response','http_error','slow_response')}
    if not recent:state='no_recent_data'
    elif counts['no_response']:state='no_response'
    elif counts['http_error']:state='http_error'
    elif counts['slow_response']:state='slow_response'
    else:state='within_limit' if limit is not None else 'responses_received_no_limit'
    events=[{'time':r['received'],'causes':[c] if (c:=response_cause(r,limit)) else []} for r in rows]
    mevents=[e for e in model_events(observations) if e['time']<=observed_until]
    response_incidents=group_events(events,ended)
    model_incidents=group_events(mevents,ended,max_gap=max(5,delivery_rows*3))
    return {'run':run['id'],'run_status':run['status'],'observation_ended':ended is not None,
            'observed_until':observed_until,'policy':policy,'recent_window_seconds':FRESHNESS_SECONDS,
            'response_state':state,'response_counts':counts,'recent_request_count':len(recent),
            'last_response_time':rows[-1]['received'] if rows else None,
            'last_response_ms':rows[-1]['duration_ms'] if rows else None,
            'recent_mean_ms':sum(r['duration_ms'] for r in recent)/len(recent) if recent else None,
            'recent_max_ms':max((r['duration_ms'] for r in recent),default=None),
            'response_incidents':response_incidents,'model_incidents':model_incidents,
            'model_signal_observations':sum(e['count'] for e in mevents),
            'model_last_decision_time':mevents[-1]['time'] if mevents else None,
            'model_data_stale':bool(mevents) and observed_until-mevents[-1]['time']>max(FRESHNESS_SECONDS,delivery_rows*3),
            'view_notice':'Группы построены по текущим правилам сервиса. Исходные ответы и решения IPCA не изменены.'}
