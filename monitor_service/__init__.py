"""Local server-metric monitoring prototype; independent of closed experiments."""
import os,sys
from pathlib import Path
for key in ('OPENBLAS_NUM_THREADS','OMP_NUM_THREADS','MKL_NUM_THREADS'):
    os.environ.setdefault(key,'1')
sys.dont_write_bytecode=True
local_dependencies=Path(__file__).resolve().parents[1]/'.research-deps'
if local_dependencies.is_dir():sys.path.insert(0,str(local_dependencies))

