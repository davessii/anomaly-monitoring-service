"""Read-only OS metrics. No packages installed, no workload/fault injection."""
import os,time,ctypes,json,urllib.request,argparse
from pathlib import Path

class WindowsMetrics:
    def __init__(self):
        from ctypes import wintypes as w
        class FT(ctypes.Structure):_fields_=[('low',w.DWORD),('high',w.DWORD)]
        class PI(ctypes.Structure):
            _fields_=[('cb',w.DWORD)]+[(k,ctypes.c_size_t) for k in ('CommitTotal','CommitLimit','CommitPeak','PhysicalTotal','PhysicalAvailable','SystemCache','KernelTotal','KernelPaged','KernelNonpaged','PageSize')]+[(k,w.DWORD) for k in ('HandleCount','ProcessCount','ThreadCount')]
        self.FT,self.PI=FT,PI
        self.kernel=ctypes.WinDLL('kernel32',use_last_error=True)
        self.psapi=ctypes.WinDLL('psapi',use_last_error=True)
        self.kernel.GetSystemTimes.argtypes=[ctypes.POINTER(FT)]*3
        self.kernel.GetSystemTimes.restype=w.BOOL
        self.psapi.GetPerformanceInfo.argtypes=[ctypes.POINTER(PI),w.DWORD]
        self.psapi.GetPerformanceInfo.restype=w.BOOL
        self.last=self.times()
    def times(self):
        idle,kernel,user=self.FT(),self.FT(),self.FT()
        if not self.kernel.GetSystemTimes(ctypes.byref(idle),ctypes.byref(kernel),ctypes.byref(user)):raise ctypes.WinError(ctypes.get_last_error())
        num=lambda x:(x.high<<32)+x.low
        return num(idle),num(kernel)+num(user)
    def sample(self):
        now=self.times();idle,total=now[0]-self.last[0],now[1]-self.last[1];self.last=now
        p=self.PI();p.cb=ctypes.sizeof(p)
        if not self.psapi.GetPerformanceInfo(ctypes.byref(p),p.cb):raise ctypes.WinError(ctypes.get_last_error())
        return {'cpu_percent':max(0.,min(100.,100*(1-idle/total))) if total>0 else 0.,
          'memory_percent':100*(1-p.PhysicalAvailable/p.PhysicalTotal),
          'commit_percent':100*p.CommitTotal/p.CommitLimit,
          'process_count':float(p.ProcessCount),'thread_count':float(p.ThreadCount),'handle_count':float(p.HandleCount)}

class LinuxMetrics:
    def __init__(self):self.last=self.times()
    def times(self):
        v=[int(x) for x in Path('/proc/stat').read_text().splitlines()[0].split()[1:]]
        return v[3]+(v[4] if len(v)>4 else 0),sum(v[:8])
    def sample(self):
        current=self.times();idle,total=current[0]-self.last[0],current[1]-self.last[1];self.last=current
        mem={x.split(':')[0]:int(x.split()[1]) for x in Path('/proc/meminfo').read_text().splitlines()}
        return {'cpu_percent':max(0.,min(100.,100*(1-idle/total))) if total>0 else 0.,
           'memory_percent':100*(1-mem['MemAvailable']/mem['MemTotal'])}

def metrics():
    if os.name=='nt':return WindowsMetrics()
    if Path('/proc/stat').exists():return LinuxMetrics()
    raise RuntimeError('В этой версии сборщик поддерживает Windows и Linux.')

def post(url,data):
    req=urllib.request.Request(url,data=json.dumps(data,allow_nan=False).encode(),headers={'Content-Type':'application/json'},method='POST')
    with urllib.request.urlopen(req,timeout=30) as response:return json.load(response)

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--url',default='http://127.0.0.1:8765');parser.add_argument('--host-id',default='local-cli');args=parser.parse_args()
    probe=metrics();sample=probe.sample()
    post(args.url+'/api/hosts',{'id':args.host_id,'name':'Сборщик '+args.host_id,'source':'local','features':list(sample)})
    print('Сбор показателей начат; Ctrl+C — остановить.')
    try:
        while True:
            time.sleep(1)
            result=post(args.url+f'/api/hosts/{args.host_id}/observations',{'observations':[{'timestamp':time.time(),'values':probe.sample()}]})
            print(result['phase'],result['samples'],flush=True)
    except KeyboardInterrupt:pass
if __name__=='__main__':main()

