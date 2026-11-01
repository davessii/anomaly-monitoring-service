"""Development mode: score each received row; learn/calibrate after 16 rows.

The IPCA core is unchanged. This is a different delivery procedure, not a
bitwise-equivalent A0 replay or an established improvement in detection quality.
The model, reference histories and auxiliary cut remain fixed within each block.
Only the preceding raw observation advances for the change scores.
"""
from .detector import Detector, DELIVERY, HISTORY, cut, t2, np
import math

MODE = 'per_observation_v1'

class ImmediateDetector(Detector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.live_block = {'base':False, 'peak':0.}

    def observe(self, timestamp, values):
        if not self.ready:
            return super().observe(timestamp, values)
        # Score exclusively against the model/histories from preceding blocks.
        x = (np.asarray([values], float) - self.center) / self.scale
        previous = ((np.asarray(self.pending[-1][1], float)-self.center)/self.scale
                    if self.pending else self.previous)
        delta = x - previous
        centered = x-self.model.mean
        error = centered-(centered@self.model.components.T)@self.model.components
        tref = max(float(np.quantile(t2(self.history,self.model),.99)),1e-12)
        rate = np.quantile(self.rates,.99,axis=0)
        rate = np.where(rate<1e-8,1.,rate)
        ratio = np.abs(delta)/rate
        ref = max(float(np.quantile(self.model.score(self.history),.99)),1e-12)
        scores = np.column_stack((self.model.score(x)/ref,t2(x,self.model)/tref,ratio.max(1)))
        er = np.column_stack((np.mean(delta**2,axis=1),np.max(error**2,axis=1)))
        if not np.isfinite(scores).all() or not np.isfinite(er).all():
            raise ValueError('Показатели дают недопустимые численные оценки.')
        base,aux = self.route({'scores':scores,'er':er,'confirmed':(ratio>1).sum(1)>=2})
        threshold = cut(self.records)
        top = np.argsort(error[0]**2)[-3:][::-1]
        decision = {'timestamp':timestamp,'decision_source_time':timestamp,
                    'alarm':bool(base[0] or aux[0]>threshold),'base_alarm':bool(base[0]),
                    'q_score':float(scores[0,0]/self.channel_scales[0]),
                    'auxiliary_score':float(aux[0]),
                    'auxiliary_threshold':threshold if math.isfinite(threshold) else None,
                    'largest_residual_features':[self.features[j] for j in top]}
        # Changes take effect only after this decision has been calculated.
        self.seen += 1
        self.pending.append([timestamp,list(values)])
        self.live_block['base'] = bool(self.live_block['base'] or base[0])
        self.live_block['peak'] = max(self.live_block['peak'],float(aux[0]))
        if len(self.pending)==DELIVERY:
            block=(np.asarray([r[1] for r in self.pending],float)-self.center)/self.scale
            changes=block-np.vstack((self.previous[None],block[:-1]))
            self.model.forgetting=self.cfg.forgetting**(len(block)/self.cfg.batch_size)
            self.model.update(block)
            self.history=np.vstack((self.history,block))[-self.cfg.history_size:]
            self.rates=np.vstack((self.rates,np.abs(changes)))[-self.cfg.history_size:]
            self.previous=block[-1].copy()
            self.records=(self.records+[dict(self.live_block)])[-HISTORY:]
            self.pending=[]
            self.live_block={'base':False,'peak':0.}
        return [decision]

    def to_state(self):
        state=super().to_state()
        state.update(processing_mode=MODE,live_block=dict(self.live_block))
        return state

    @classmethod
    def from_state(cls,state):
        obj=super().from_state(state)
        obj.live_block=dict(state['live_block'])
        return obj

def restore_detector(state):
    mode=state.get('processing_mode','batch16')
    if mode=='log_quality_v2':
        from .log_detector import LogDetector
        return LogDetector.from_state(state)
    if mode==MODE:return ImmediateDetector.from_state(state)
    if mode=='batch16':return Detector.from_state(state)
    raise ValueError('Неизвестный режим обработки данных.')
