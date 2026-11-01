"""Persisted log detector v2: quality-gated learning and typed messages.

Statistical warnings are not labelled proven faults. Quality recovery requires
ten consecutive complete good windows; missing data never proves recovery.
The nested IPCA implementation and old registered modes remain unchanged.
"""
import copy
import math
from .immediate import ImmediateDetector
from .log_quality_guard_candidate_v4 import QualityGuardCandidate
from .log_schema import FEATURES

MODE='log_quality_v2'
KINDS={'training','unfit','alarm','warning','recovering','recovery','normal'}
TITLES={'training':'Модель обучается','unfit':'Настройка непригодна','alarm':'Тревога',
        'warning':'Предупреждение','recovering':'Проверяется восстановление',
        'recovery':'Обнаружено восстановление','normal':'Работа в пределах требований'}


class LogDetector:
    def __init__(self,features,components=1,fit_rows=256,reference_rows=128,validation_rows=512,
                 *,handler_limit_ms,allowed_error_fraction=0.):
        if tuple(features)!=FEATURES or type(components) is not int or components!=1:raise ValueError('Для журнала нужны три заданных показателя и одно направление.')
        self.candidate=QualityGuardCandidate(handler_limit_ms=handler_limit_ms,
                                              allowed_error_fraction=allowed_error_fraction)
        self.candidate.detector=ImmediateDetector(features,1,fit_rows,reference_rows,validation_rows)
        self.log_settings=dict(version=2,handler_limit_ms=float(handler_limit_ms),
                               allowed_error_fraction=float(allowed_error_fraction),
                               recovery_seconds=10,protect_learning=True)
        self.received=0
        self.invalid_reason=None
        self.quality_state=dict(open=False,started=None,last_bad=None,good_count=0,last_ts=None)

    @property
    def core(self):return self.candidate.detector
    @property
    def features(self):return self.core.features
    @property
    def cfg(self):return self.core.cfg
    @property
    def lengths(self):return self.core.lengths
    @property
    def required(self):return self.core.required
    @property
    def pending(self):return self.core.pending
    @property
    def seen(self):return self.received
    @property
    def ready(self):return bool(self.core.ready and self.candidate.readiness['eligible'] and not self.invalid_reason)
    @property
    def phase(self):
        return 'unfit' if self.invalid_reason or self.core.ready and not self.candidate.readiness['eligible'] else 'monitoring' if self.ready else 'warmup'

    def observe(self,timestamp,values):
        if (type(timestamp) not in (int,float) or not math.isfinite(timestamp) or timestamp<=0
                or self.quality_state['last_ts'] is not None and timestamp<=self.quality_state['last_ts']):
            raise ValueError('Время журнала должно строго возрастать.')
        metrics=dict(zip(self.features,values))
        if len(values)!=3:raise ValueError('Нужны три показателя журнала.')
        from .log_candidate_v2 import _values
        _values(metrics)
        slow=metrics['mean_request_ms']>self.log_settings['handler_limit_ms']
        error=metrics['server_error_fraction']>self.log_settings['allowed_error_fraction']
        quality_bad=bool(slow or error)
        if not self.core.ready and quality_bad:self.invalid_reason='quality_violation_during_warmup'
        result=None
        if not self.invalid_reason:result=self.candidate.observe(timestamp,metrics)
        self.received+=1
        ipca=result['ipca_decision'] if result else None
        statistical=bool(ipca and ipca['alarm'])
        q=self.quality_state
        recovered=False
        if quality_bad:
            if not q['open']:q['started']=timestamp
            q.update(open=True,last_bad=timestamp,good_count=0)
        elif q['open']:
            q['good_count']=q['good_count']+1 if q['last_ts'] is not None and timestamp-q['last_ts']==1 else 1
            if q['good_count']>=self.log_settings['recovery_seconds']:
                q['open']=False;recovered=True
        q['last_ts']=timestamp
        if quality_bad:
            kind='alarm'
            parts=[]
            if slow:parts.append(f"Среднее время обработки {metrics['mean_request_ms']:.3f} мс выше {self.log_settings['handler_limit_ms']:g} мс.")
            if error:parts.append(f"Ответы 500–599: {metrics['server_error_fraction']*100:.3f}%; допустимо {self.log_settings['allowed_error_fraction']*100:g}%.")
            message=' '.join(parts)
        elif recovered:
            kind='recovery';message='Десять последовательных секунд ответы соответствуют заданным требованиям. Это восстановление качества ответов, а не доказательство исправности модели.'
        elif self.phase=='unfit':
            kind='unfit';message=('При начальной настройке были нарушения качества ответов. Создайте новое наблюдение на обычной работе.' if self.invalid_reason else 'Начальные данные не дали пригодной настройки ошибки восстановления. Нужна проверка источника и новое наблюдение.')
        elif q['open']:
            kind='recovering';message=f"Ответы снова в пределах требований: {q['good_count']} из 10 последовательных секунд."
        elif self.phase=='warmup':
            kind='training';message=f'Начальная настройка: {self.core.seen} из {self.required} наблюдений.'
        elif statistical:
            kind='warning';message='Модель заметила необычные показатели. Заданные требования к ответам не нарушены; причина изменения не установлена.'
        else:
            kind='normal';message='Заданные требования к ответам соблюдены; нового сигнала модели нет.'
        decision=dict(ipca or dict(timestamp=timestamp,decision_source_time=timestamp,base_alarm=False,
                                   q_score=None,auxiliary_score=None,auxiliary_threshold=None,
                                   largest_residual_features=[]))
        decision.update(alarm=bool(statistical or quality_bad),statistical_alarm=statistical,
                        quality_alarm=quality_bad,http_error_alarm=bool(error),handler_time_alarm=bool(slow),
                        message_kind=kind,message_title=TITLES[kind],message=message,
                        learning=result['learning'] if result else dict(attempted=False,accepted=False,deferred=False),
                        update_counts=dict(self.candidate.update_counts),model_available=self.ready,
                        phase=self.phase,quality_state=dict(q),processing_mode=MODE)
        return [decision]

    def to_state(self):
        c=self.candidate
        context={name:copy.deepcopy(getattr(c,name)) for name in ('readiness','last_timestamp','normal_bounds',
                 'previous_metrics','previous_result','block_suspect','block_quality_bad','update_counts')}
        return dict(version=2,processing_mode=MODE,features=list(self.features),config=self.core.to_state()['config'],
                    lengths=list(self.lengths),seen=self.seen,ready=self.ready,phase=self.phase,
                    log_settings=dict(self.log_settings),core=self.core.to_state(),candidate=context,
                    invalid_reason=self.invalid_reason,quality_state=dict(self.quality_state))

    @classmethod
    def from_state(cls,state):
        state=copy.deepcopy(state)
        expected={'version','processing_mode','features','config','lengths','seen','ready','phase','log_settings',
                  'core','candidate','invalid_reason','quality_state'}
        if set(state)!=expected or state['version']!=2 or state['processing_mode']!=MODE:raise ValueError('Неизвестное состояние модели журнала.')
        settings=state['log_settings']
        if (set(settings)!={'version','handler_limit_ms','allowed_error_fraction','recovery_seconds','protect_learning'}
                or settings['version']!=2 or settings['recovery_seconds']!=10 or settings['protect_learning'] is not True):
            raise ValueError('Неизвестные параметры журнала.')
        obj=cls(state['features'],state['config']['components'],*state['lengths'],
                handler_limit_ms=settings['handler_limit_ms'],allowed_error_fraction=settings['allowed_error_fraction'])
        obj.candidate.detector=ImmediateDetector.from_state(state['core'])
        if (obj.core.features!=state['features'] or obj.core.lengths!=state['lengths']
                or obj.core.to_state()['config']!=state['config']):raise ValueError('Параметры вложенной модели не совпали.')
        keys={'readiness','last_timestamp','normal_bounds','previous_metrics','previous_result',
              'block_suspect','block_quality_bad','update_counts'}
        if set(state['candidate'])!=keys:raise ValueError('Неполное состояние допуска обновлений.')
        for key,value in state['candidate'].items():setattr(obj.candidate,key,value)
        if (type(state['seen']) is not int or state['seen']<obj.core.seen or type(state['ready']) is not bool
                or state['invalid_reason'] not in (None,'quality_violation_during_warmup')):
            raise ValueError('Неверное состояние обучения.')
        obj.received=state['seen'];obj.invalid_reason=state['invalid_reason']
        quality=state['quality_state']
        if (set(quality)!={'open','started','last_bad','good_count','last_ts'} or type(quality['open']) is not bool
                or type(quality['good_count']) is not int or not 0<=quality['good_count']<=10):raise ValueError('Неверное состояние восстановления.')
        obj.quality_state=quality
        if obj.ready!=state['ready'] or obj.phase!=state['phase']:raise ValueError('Статус модели не совпал с состоянием.')
        return obj
