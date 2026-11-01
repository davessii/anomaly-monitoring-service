"""Persistable A0 adapter. Score BEFORE learning; delivery remains 16 rows.

IPCA implementation is copied unchanged from the fixed reference. Q/T/D and E/R
follow stage2 scoring. This service has a configurable schema/warmup, so new
server quality is NOT inferred from the closed SMD study. Pdelta is not enabled.
"""
from dataclasses import asdict
import math
import numpy as np
from ._reference_models import Config,IncrementalSubspace

DELIVERY=16
HISTORY=32

def cut(records,fraction=.25):
    count=sum(r['base'] for r in records)
    budget=int(np.floor(fraction*count))
    eligible=sorted(r['peak'] for r in records if not r['base'])
    return (float('inf') if not eligible else 0. if budget>=len(eligible)
            else max(float(eligible[len(eligible)-budget-1]),1e-12))

def t2(x,model):
    variance=model.singular**2/model.mass
    floor=max(1e-12,1e-8*float(variance.max()))
    return np.sum(((x-model.mean)@model.components.T)**2/np.maximum(variance,floor),axis=1)

class Detector:
    def __init__(self,features,components=None,fit_rows=256,reference_rows=128,validation_rows=512):
        self.features=list(features)
        k=min(5,len(features)-1) if components is None else components
        if type(k) is not int or not 1<=k<=min(16,len(features)-1):raise ValueError('Число направлений: от 1 до 16 и меньше числа показателей.')
        if any(type(n) is not int or n<16 or n%16 for n in (fit_rows,reference_rows,validation_rows)):
            raise ValueError('Длины начальных частей должны быть положительными кратными 16.')
        if validation_rows<512 or max(fit_rows,reference_rows,validation_rows)>16384:
            raise ValueError('Настройка требует не менее 512 строк; максимальная длина части — 16384.')
        self.cfg=Config(components=k)
        self.lengths=[fit_rows,reference_rows,validation_rows]
        self.warmup=[]
        self.pending=[]
        self.seen=0
        self.ready=False

    @property
    def required(self):return sum(self.lengths)

    def initialize(self):
        raw=np.asarray(self.warmup,float)
        a,b=self.lengths[:2]
        fit,reference,validation=raw[:a],raw[a:a+b],raw[a+b:]
        self.center=fit.mean(0)
        std=fit.std(0);self.scale=np.where(std<1e-8,1.,std)
        normalized=(fit-self.center)/self.scale
        self.model=IncrementalSubspace(self.cfg.components,self.cfg.forgetting)
        for part in np.array_split(normalized,max(1,len(normalized)//self.cfg.batch_size)):
            self.model.update(part)
        ref=(reference-self.center)/self.scale
        prev=normalized[-1]
        self.history=ref[-self.cfg.history_size:].copy()
        self.rates=np.abs(ref-np.vstack((prev[None],ref[:-1])))[-self.cfg.history_size:].copy()
        self.previous=ref[-1].copy()
        values=[self.trace(validation[i:i+16]) for i in range(0,len(validation),16)]
        scores=np.concatenate([v['scores'] for v in values])
        self.channel_scales=np.array([max(float(np.quantile(scores[:,i],.99,method='higher')),1e-12) for i in range(3)])
        q=scores[:,0]>self.channel_scales[0]
        td=np.max(scores[:,1:]/self.channel_scales[1:],axis=1)
        records=[{'base':bool(q[i:i+16].any()),'peak':float(td[i:i+16].max())} for i in range(0,len(q),16)]
        self.background_cut=cut(records)
        self.er_scales=np.maximum(np.quantile(np.concatenate([v['er'] for v in values]),.99,axis=0,method='higher'),1e-12)
        self.records=[]
        for v in values:
            base,aux=self.route(v)
            self.records.append({'base':bool(base.any()),'peak':float(aux.max())})
        self.records=self.records[-HISTORY:]
        self.ready=True
        self.warmup=[]

    def trace(self,raw):
        x=(np.asarray(raw,float)-self.center)/self.scale
        delta=x-np.vstack((self.previous[None],x[:-1]))
        centered=x-self.model.mean
        error=centered-(centered@self.model.components.T)@self.model.components
        energy=np.mean(delta**2,axis=1)
        residual=np.max(error**2,axis=1)
        t=t2(x,self.model)
        tref=max(float(np.quantile(t2(self.history,self.model),.99)),1e-12)
        rate=np.quantile(self.rates,.99,axis=0)
        rate=np.where(rate<1e-8,1.,rate)
        ratio=np.abs(delta)/rate
        q=self.model.score(x)
        ref=max(float(np.quantile(self.model.score(self.history),.99)),1e-12)
        scores=np.column_stack((q/ref,t/tref,ratio.max(1)))
        if not np.isfinite(scores).all():raise ValueError('Показатели дают недопустимые численные оценки.')
        result={'scores':scores,'er':np.column_stack((energy,residual)),
                'confirmed':(ratio>1).sum(1)>=2,'residuals':error**2}
        # Changing the model/history affects only the next delivery.
        self.model.forgetting=self.cfg.forgetting**(len(x)/self.cfg.batch_size)
        self.model.update(x)
        self.history=np.vstack((self.history,x))[-self.cfg.history_size:]
        self.rates=np.vstack((self.rates,np.abs(delta)))[-self.cfg.history_size:]
        self.previous=x[-1].copy()
        return result

    def route(self,v):
        s=v['scores']
        base=(s[:,0]>self.channel_scales[0])|((np.maximum(s[:,1]/self.channel_scales[1],s[:,2]/self.channel_scales[2])>self.background_cut)&v['confirmed'])
        return base,np.max(v['er']/self.er_scales,axis=1)

    def observe(self,timestamp,values):
        self.seen+=1
        if not self.ready:
            self.warmup.append(list(values))
            if len(self.warmup)==self.required:self.initialize()
            return []
        self.pending.append([timestamp,list(values)])
        if len(self.pending)<DELIVERY:return []
        rows=self.pending
        threshold=cut(self.records) # Do not let this delivery set its own cut.
        v=self.trace([r[1] for r in rows])
        base,aux=self.route(v)
        alarm=base|(aux>threshold)
        result=[]
        for i,(ts,_) in enumerate(rows):
            top=np.argsort(v['residuals'][i])[-3:][::-1]
            result.append({'timestamp':ts,'decision_source_time':rows[-1][0],
              'alarm':bool(alarm[i]),'base_alarm':bool(base[i]),
              'q_score':float(v['scores'][i,0]/self.channel_scales[0]),
              'auxiliary_score':float(aux[i]),'auxiliary_threshold':threshold if math.isfinite(threshold) else None,
              'largest_residual_features':[self.features[j] for j in top]})
        self.records=(self.records+[{'base':bool(base.any()),'peak':float(aux.max())}])[-HISTORY:]
        self.pending=[]
        return result

    def to_state(self):
        state={'version':1,'features':self.features,'config':asdict(self.cfg),'lengths':self.lengths,
           'warmup':self.warmup,'pending':self.pending,'seen':self.seen,'ready':self.ready}
        if self.ready:
            for key in ('center','scale','history','rates','previous','channel_scales','er_scales'):
                state[key]=getattr(self,key).tolist()
            state['background_cut']=self.background_cut if math.isfinite(self.background_cut) else None
            state['records']=self.records
            state['model']={'mass':self.model.mass,'forgetting':self.model.forgetting,
                **{k:getattr(self.model,k).tolist() for k in ('mean','components','singular')}}
        return state

    @classmethod
    def from_state(cls,state):
        if state['version']!=1:raise ValueError('Неизвестная версия состояния.')
        obj=cls(state['features'],state['config']['components'],*state['lengths'])
        obj.cfg=Config(**state['config'])
        for k in ('warmup','pending','seen','ready'):setattr(obj,k,state[k])
        if obj.ready:
            for k in ('center','scale','history','rates','previous','channel_scales','er_scales'):
                setattr(obj,k,np.asarray(state[k],float))
            obj.background_cut=float('inf') if state['background_cut'] is None else state['background_cut']
            obj.records=state['records']
            obj.model=IncrementalSubspace(obj.cfg.components,state['model']['forgetting'])
            obj.model.mass=state['model']['mass']
            for k in ('mean','components','singular'):setattr(obj.model,k,np.asarray(state['model'][k],float))
        return obj

