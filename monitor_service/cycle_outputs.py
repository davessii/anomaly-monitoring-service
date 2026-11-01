"""Save four cycle-profile outputs before any quality evaluation.

Inputs: chronological JSONL raw counter snapshots, first row an explicit anchor.
No labels/outcomes read, no collection, no claim of measurement admission here.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys
from .cycle_profile import CycleComparison,FEATURES,PROFILE,project,np

NAMES=('ipca','separate_limits','cycles_per_request','expected_cycles')


def encode(value):return json.dumps(value,ensure_ascii=False,allow_nan=False,separators=(',',':'))

def sha(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def save_outputs(counters,output):
    output=Path(output).resolve();output.mkdir(parents=True,exist_ok=False)
    engine=CycleComparison();files={};count=inputs=0
    metadata={'profile':PROFILE,'features':list(FEATURES),'anchor_rows':1,'warmup_input_rows':896,
              'resource_unit':'cycles','python':sys.version.split()[0],'numpy':np.__version__,
              'measurement_admission_asserted':False,'quality_evaluated':False,
              'timing_scope':'Own computation sections, no full-service or browser latency measurement.'}
    package=Path(__file__).resolve().parent
    metadata['code_sha256']={name:sha(package/name) for name in ('cycle_outputs.py','cycle_profile.py',
                            'efficiency_baselines.py','immediate.py','detector.py','_reference_models.py')}
    (output/'metadata.json').write_text(encode(metadata),encoding='utf-8')
    try:
        for name in (*NAMES,'projected_measurements','comparison_trace'):
            files[name]=(output/(name+'.jsonl')).open('x',encoding='utf-8')
        for raw in counters:
            clean=project(raw)
            arrival=raw.get('collector_received_at')
            if arrival is not None and (type(arrival) not in (int,float) or not np.isfinite(arrival) or arrival<=0):
                raise ValueError('Invalid collector arrival metadata.')
            files['projected_measurements'].write(encode(dict(clean,collector_received_at=arrival))+'\n')
            inputs+=1
            result=engine.observe(clean)
            if result is None:continue
            count+=1
            # Arrival metadata does not affect numeric model inputs.
            result['collector_received_at']=arrival
            for name in NAMES:
                record=dict(result['decisions'][name],profile=PROFILE,timestamp=result['timestamp'],
                            collector_received_at=arrival)
                files[name].write(encode(record)+'\n')
            files['comparison_trace'].write(encode(result)+'\n')
        if not count:raise ValueError('No post-warmup decisions; do not create a complete barrier.')
        for stream in files.values():stream.close()
        (output/'comparison_state.json').write_text(encode(engine.to_state()),encoding='utf-8')
        (output/'scoring_status.json').write_text(encode({'status':'scoring_complete','input_snapshots':inputs,
                    'post_warmup_rows':count,'quality_evaluated':False}),encoding='utf-8')
        paths=[p for p in output.iterdir() if p.is_file()]
        barrier={'profile':PROFILE,'input_snapshots':inputs,'post_warmup_rows':count,
                 'quality_evaluated':False,'measurement_admission_asserted':False,
                 'files':[{'path':p.name,'bytes':p.stat().st_size,'sha256':sha(p)} for p in sorted(paths)]}
        (output/'four_outputs_complete.json').write_text(encode(barrier),encoding='utf-8')
        verify_outputs(output)
        return barrier
    except BaseException as exc:
        for stream in files.values():stream.close()
        marker=output/'four_outputs_complete.json'
        if marker.exists():marker.unlink() # Only our new output marker, never existing user files.
        (output/'scoring_status.json').write_text(encode({'status':'failed','error':type(exc).__name__+': '+str(exc),
                                              'quality_evaluated':False}),encoding='utf-8')
        raise


def verify_outputs(output):
    output=Path(output).resolve();barrier=json.loads((output/'four_outputs_complete.json').read_text(encoding='utf-8'))
    if barrier.get('profile')!=PROFILE or barrier.get('post_warmup_rows',0)<1:raise ValueError('Invalid output barrier.')
    if barrier.get('input_snapshots')!=barrier['post_warmup_rows']+897:raise ValueError('Input and output progress differ.')
    listed={record['path'] for record in barrier['files']}
    required={name+'.jsonl' for name in (*NAMES,'projected_measurements','comparison_trace')}|{'metadata.json','comparison_state.json','scoring_status.json'}
    if listed!=required or len(listed)!=len(barrier['files']):raise ValueError('Incomplete output file list.')
    for record in barrier['files']:
        path=(output/record['path']).resolve()
        if not path.is_relative_to(output) or not path.is_file() or path.stat().st_size!=record['bytes'] or sha(path)!=record['sha256']:
            raise ValueError('Output content differs from the complete barrier.')
    for name in NAMES:
        with (output/(name+'.jsonl')).open(encoding='utf-8') as stream:
            if sum(1 for _ in stream)!=barrier['post_warmup_rows']:raise ValueError('Method output counts differ.')
    return barrier


def main():
    parser=argparse.ArgumentParser(description='Только сохранение четырёх выходов на подготовленном файле измерений, без оценки качества.')
    parser.add_argument('--input',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    with args.input.open(encoding='utf-8') as stream:
        def rows():
            for line in stream:
                yield json.loads(line,parse_constant=lambda v:(_ for _ in ()).throw(ValueError('Non-finite JSON.')))
        save_outputs(rows(),args.output)
    print('Четыре выхода сохранены. Качество и допуск измерений не оценивались.')


if __name__=='__main__':main()
