"""Read the reviewed plan, never grant collection permission or mutate it."""
import hashlib
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
SPEC=Path(__file__).with_name('threshold_guard_protocol_v01.json')
SEAL=Path(__file__).with_name('threshold_guard_protocol_v01.sha256.json')
NAMES=('ipca_reference','separate_limits_reference','ratio_fixed','ratio_updated','ratio_limited',
       'expected_fixed','expected_updated','expected_limited')


def sha(path):
    with Path(path).open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def load_protocol():
    seal=json.loads(SEAL.read_text(encoding='utf-8'))
    for name,expected in seal['files'].items():
        path=(ROOT/name).resolve()
        if not path.is_relative_to(ROOT) or sha(path)!=expected:raise ValueError('Reviewed protocol files changed.')
    if str(SPEC.relative_to(ROOT)) not in seal['files']:raise ValueError('Protocol is not covered by its seal.')
    cfg=json.loads(SPEC.read_text(encoding='utf-8'))
    if (tuple(cfg['outputs'])!=NAMES or cfg['threshold_policy']['upward_step_fraction_of_initial']!=.10
            or cfg['threshold_policy']['update_every_input_rows']!=16 or cfg['calibration']['total_rows']!=896):
        raise ValueError('Unsupported protocol version; implement and review separately.')
    return cfg
