"""Isolated cycles profile: same raw input, three baselines and existing IPCA.

No workload is started on import. No outcome/command metadata is an input.
The old CPU-time profile and mathematical detector implementation are unchanged.
"""
from bisect import bisect_right
from collections import deque
import copy
import json
import math
from pathlib import Path
import time
import numpy as np
from .bench import Bench
from .efficiency_baselines import SimpleComparisons, METHODS, WARMUP
from .immediate import ImmediateDetector, MODE

PROFILE = 'cycles_v1'
FEATURES = ('app_cpu_cycles_per_second', 'app_memory_mib', 'active_requests', 'completed_per_second')
METHOD_IDS = {'separate_limits': 'separate_limits', 'cpu_per_request': 'cycles_per_request', 'expected_cpu': 'expected_cycles'}


def project(raw):
    """Retain only real measurement fields. Never convert seconds to cycles."""
    if raw.get('metrics_schema') != 'application_cycles_snapshot_v1' or raw.get('cpu_cycles_status') != 'available':
        raise ValueError('Cycles profile requires an available application_cycles_snapshot_v1 counter.')
    if raw.get('cpu_cycles_source') != 'QueryProcessCycleTime' or raw.get('cpu_cycles_unit') != 'cycles':
        raise ValueError('Cycle source or unit mismatch.')
    if raw.get('cpu_cycles_error') is not None:
        raise ValueError('Cycle reader reported an error.')
    clean = {key: raw[key] for key in ('metrics_schema', 'cpu_cycles_status', 'cpu_cycles_source', 'cpu_cycles_unit')}
    for key in ('cpu_cycles', 'cpu_cycles_after_snapshot', 'cpu_cycles_read_span_ns', 'requests_completed', 'active_requests'):
        value = raw[key]
        if type(value) is not int or value < 0:
            raise ValueError('Expected a nonnegative integer: ' + key)
        clean[key] = value
    if clean['cpu_cycles_after_snapshot'] < clean['cpu_cycles']:
        raise ValueError('Cycle bracket decreased.')
    for key in ('timestamp', 'memory_mib'):
        value = raw[key]
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError('Invalid numeric measurement: ' + key)
        clean[key] = value
    if not 0 < clean['timestamp'] < 1e11:
        raise ValueError('Invalid timestamp.')
    return clean


def check_pair(previous, current):
    dt = current['timestamp'] - previous['timestamp']
    if not 0 < dt <= 3:
        raise ValueError('Measurements must advance with no gap over three seconds.')
    if current['cpu_cycles'] < previous['cpu_cycles_after_snapshot']:
        raise ValueError('Cycle reset or overlapping reading brackets.')
    if current['requests_completed'] < previous['requests_completed']:
        raise ValueError('Completion counter reset.')
    return dt


def cycle_increment(previous, current):
    lower = current['cpu_cycles'] - previous['cpu_cycles_after_snapshot']
    upper = current['cpu_cycles_after_snapshot'] - previous['cpu_cycles']
    if lower < 0 or upper < lower:
        raise ValueError('Invalid cycle interval.')
    # Subtract integers BEFORE converting: cumulative counters can exceed 2**53.
    midpoint = (lower + upper) / 2
    return lower, upper, midpoint


def detector_values(previous, current):
    previous, current = project(previous), project(current)
    dt = check_pair(previous, current)
    _, _, cycles = cycle_increment(previous, current)
    result = dict(zip(FEATURES, (cycles / dt, current['memory_mib'], float(current['active_requests']),
                               (current['requests_completed'] - previous['requests_completed']) / dt)))
    if not all(math.isfinite(v) and abs(v) <= 1e12 for v in result.values()):
        raise ValueError('Cycles profile values exceed the Store numeric range.')
    return result


class CycleWindow:
    def __init__(self):
        self.rows = deque()

    def observe(self, raw):
        row = project(raw)
        if self.rows:
            check_pair(self.rows[-1], row)
        self.rows.append(row)
        while len(self.rows) > 1 and self.rows[1]['timestamp'] <= row['timestamp'] - 13:
            self.rows.popleft()
        rows = list(self.rows)
        index = bisect_right([r['timestamp'] for r in rows], row['timestamp'] - 10) - 1
        if index < 0:
            return {'timestamp': row['timestamp'], 'valid': False, 'reason': 'window_not_ready'}
        part = rows[index:]
        start, end = part[0], part[-1]
        dt = end['timestamp'] - start['timestamp']
        if not 10 <= dt <= 13:
            raise ValueError('Window endpoints outside 10..13 seconds.')
        lo, hi, cycles = cycle_increment(start, end)
        completed = end['requests_completed'] - start['requests_completed']
        mean = lambda key: sum((b['timestamp'] - a['timestamp']) * b[key] for a, b in zip(part, part[1:])) / dt
        width = (hi - lo) / cycles if cycles > 0 else None
        return {'timestamp': end['timestamp'], 'window_start': start['timestamp'], 'duration_seconds': dt,
                'valid': True, 'reason': None, 'resource_unit': 'cycles', 'resource_cycles': cycles,
                'cycle_increment_lower': lo, 'cycle_increment_upper': hi,
                'read_alignment_full_width_fraction': width,
                'alignment_screen_passed': width is not None and width <= .05,
                'completed': completed, 'ratio_eligible': completed >= 4,
                'features': [cycles / dt, mean('memory_mib'), mean('active_requests'), completed / dt]}


class CycleSimpleComparisons(SimpleComparisons):
    """Reuse the existing chronology/cut-update procedure, explicit new units."""
    def __init__(self):
        super().__init__()
        self.window = CycleWindow()

    def fit(self):
        valid = [w for w in self.fit_windows if w['valid']]
        if len(valid) < 32:
            raise ValueError('Insufficient valid cycle windows for fitting.')
        matrix = np.asarray([w['features'] for w in valid], float)
        self.center = matrix.mean(0)
        std = matrix.std(0)
        self.scale = np.where(std < 1e-8, 1., std)
        n = np.asarray([w['completed'] for w in valid], float)
        resource = np.asarray([w['resource_cycles'] for w in valid], float)
        if len(np.unique(n)) < 2:
            raise ValueError('Distinct completion counts are required for expected cycles.')
        self.coefficients, _, rank, _ = np.linalg.lstsq(np.column_stack((np.ones(len(n)), n)), resource, rcond=None)
        if rank != 2 or not np.isfinite(self.coefficients).all():
            raise ValueError('Expected cycles fit is not identifiable.')
        self.n_range = (float(n.min()), float(n.max()))
        self.fit_diagnostics = {'resource_unit': 'cycles', 'valid_windows': len(valid),
                                'center': self.center.tolist(), 'scale': self.scale.tolist(),
                                'cycles_intercept': float(self.coefficients[0]),
                                'cycles_slope_per_completion': float(self.coefficients[1]),
                                'completed_range': list(self.n_range)}
        self.fit_windows = []

    def scores(self, window):
        scores, elapsed = {m: None for m in METHODS}, {m: 0. for m in METHODS}
        if not window['valid']:
            return scores, elapsed, None
        mark = time.perf_counter()
        scores['separate_limits'] = float(np.max(np.abs((np.asarray(window['features']) - self.center) / self.scale)))
        elapsed['separate_limits'] = time.perf_counter() - mark
        mark = time.perf_counter()
        if window['ratio_eligible']:
            scores['cpu_per_request'] = window['resource_cycles'] / window['completed']
        elapsed['cpu_per_request'] = time.perf_counter() - mark
        mark = time.perf_counter()
        prediction = float(self.coefficients[0] + self.coefficients[1] * window['completed'])
        scores['expected_cpu'] = max(0., window['resource_cycles'] - prediction)
        outside = not self.n_range[0] <= window['completed'] <= self.n_range[1]
        elapsed['expected_cpu'] = time.perf_counter() - mark
        if any(v is not None and not math.isfinite(v) for v in scores.values()):
            raise ValueError('Nonfinite cycle score.')
        return scores, elapsed, outside


class CycleComparison:
    """Four outputs; 896 input rows AFTER a saved raw anchor, fresh model only."""
    def __init__(self):
        self.previous = None
        self.simple = CycleSimpleComparisons()
        self.ipca = ImmediateDetector(FEATURES, components=2)
        self.failed = None

    def observe(self, raw):
        input_started = time.perf_counter()
        if self.failed:
            raise ValueError('Profile failed; restore a preceding saved state: ' + self.failed)
        current = project(raw)
        if self.previous is None:
            self.simple.window.observe(current)
            self.previous = current
            return None
        values = detector_values(self.previous, current) # All raw validation precedes updates.
        input_seconds = time.perf_counter() - input_started
        try:
            simple = self.simple.observe(current)
            mark = time.perf_counter()
            ipca = self.ipca.observe(current['timestamp'], [values[f] for f in FEATURES])
            ipca_seconds = time.perf_counter() - mark
        except Exception as exc:
            self.failed = type(exc).__name__ + ': ' + str(exc)
            raise
        self.previous = current
        if self.simple.seen != self.ipca.seen:
            self.failed = 'Method progress differs.'
            raise ValueError(self.failed)
        if simple is None:
            if ipca:
                raise ValueError('IPCA produced decisions during common warmup.')
            return None
        if len(ipca) != 1:
            raise ValueError('Expected one IPCA decision per observation.')
        decisions = {}
        units = {'separate_limits': 'standardized_deviation', 'cpu_per_request': 'cycles_per_completed_request', 'expected_cpu': 'cycles'}
        for old, new in METHOD_IDS.items():
            decisions[new] = dict(simple['decisions'][old], score_unit=units[old],
                                  decision_source_time=current['timestamp'])
        decisions['ipca'] = dict(ipca[0], own_compute_seconds=ipca_seconds,
                                 input_resource_unit='cycles_per_second')
        return {'profile': PROFILE, 'timestamp': current['timestamp'], 'input_values': values,
                'input_feature_names': list(FEATURES), 'window': simple['window'], 'decisions': decisions,
                'expected_cycles_extrapolation': simple['expected_cpu_extrapolation'],
                'common_input_preparation_seconds': input_seconds,
                'common_window_preparation_seconds': simple['common_preparation_seconds'],
                'simple_update_seconds': {METHOD_IDS[m]: v for m, v in simple['own_update_seconds'].items()},
                'timing_scope': 'Separate computation sections; not four independent full-service timings.',
                'alignment_gate_is_not_detection': True}

    def to_state(self):
        if self.failed:
            raise ValueError('A partially failed profile cannot be saved.')
        s = self.simple
        simple = {'seen': s.seen, 'window_rows': list(s.window.rows), 'fit_windows': s.fit_windows,
                  'histories': {m: list(v) for m, v in s.histories.items()}, 'cuts': s.cuts,
                  'pending': s.pending, 'fit_diagnostics': s.fit_diagnostics}
        if s.seen >= 256:
            simple['fitted'] = {'center': s.center.tolist(), 'scale': s.scale.tolist(),
                                'coefficients': s.coefficients.tolist(), 'n_range': list(s.n_range)}
        return copy.deepcopy({'version': 1, 'profile': PROFILE, 'features': list(FEATURES),
                              'previous': self.previous, 'simple': simple, 'ipca': self.ipca.to_state()})

    @classmethod
    def from_state(cls, state):
        if state.get('version') != 1 or state.get('profile') != PROFILE or state.get('features') != list(FEATURES):
            raise ValueError('Wrong profile/state schema; old states cannot be renamed.')
        json.dumps(state,allow_nan=False) # No NaN/Infinity in saved state.
        state = copy.deepcopy(state)
        obj = cls()
        obj.previous = None if state['previous'] is None else project(state['previous'])
        s = state['simple']
        if type(s['seen']) is not int or s['seen'] < 0 or len(s['pending']) >= 16:
            raise ValueError('Invalid progress in saved state.')
        if len(s['window_rows']) > 128 or set(s['histories']) != set(METHODS):
            raise ValueError('Invalid saved histories.')
        if any(len(v)>512 for v in s['histories'].values()):
            raise ValueError('Oversized saved score history.')
        expected_pending=max(0,s['seen']-WARMUP)%16
        if len(s['pending'])!=expected_pending:
            raise ValueError('Saved block position differs.')
        if (s['seen']>=WARMUP and set(s['cuts'])!=set(METHODS)) or (s['seen']<WARMUP and s['cuts']):
            raise ValueError('Invalid saved cut phase.')
        if any(type(v) not in (int,float) or not math.isfinite(v) or v<0 for v in s['cuts'].values()):
            raise ValueError('Invalid saved cut.')
        window = CycleWindow()
        for row in s['window_rows']:
            window.observe(row)
        obj.simple.window = window
        if (obj.previous is None)!=(not window.rows) or (window.rows and window.rows[-1]!=obj.previous):
            raise ValueError('Saved anchor and window differ.')
        obj.simple.seen = s['seen']
        obj.simple.fit_windows = s['fit_windows']
        obj.simple.histories = {m: deque(v, maxlen=512) for m, v in s['histories'].items()}
        obj.simple.cuts, obj.simple.pending = s['cuts'], s['pending']
        obj.simple.fit_diagnostics = s['fit_diagnostics']
        for values in obj.simple.histories.values():
            if len(values) > 512 or any(type(v) not in (int, float) or not math.isfinite(v) for v in values):
                raise ValueError('Invalid scores in saved history.')
        if s['seen'] >= 256:
            fit = s['fitted']
            for name, size in (('center', 4), ('scale', 4), ('coefficients', 2)):
                array = np.asarray(fit[name], float)
                if array.shape != (size,) or not np.isfinite(array).all() or (name == 'scale' and np.any(array <= 0)):
                    raise ValueError('Invalid fitted cycle state.')
                setattr(obj.simple, name, array)
            obj.simple.n_range = tuple(fit['n_range'])
            if len(obj.simple.n_range)!=2 or not all(math.isfinite(v) and v>=0 for v in obj.simple.n_range) or obj.simple.n_range[0]>obj.simple.n_range[1]:
                raise ValueError('Invalid saved completion range.')
        ipca = state['ipca']
        if ipca.get('features') != list(FEATURES) or ipca.get('processing_mode') != MODE or ipca.get('config', {}).get('components') != 2:
            raise ValueError('Wrong IPCA feature schema.')
        if ipca.get('lengths')!=[256,128,512] or ipca.get('config')!=obj.ipca.to_state()['config']:
            raise ValueError('Saved IPCA settings differ from the profile.')
        obj.ipca = ImmediateDetector.from_state(ipca)
        if obj.ipca.seen != obj.simple.seen:
            raise ValueError('Saved method progress differs.')
        return obj


class CycleBench(Bench):
    features = FEATURES
    input_profile = PROFILE

    def derive_values(self, previous, current):
        return detector_values(previous, current)

    def stamp_measurement(self, raw):
        # Timestamp directly after the complete HTTP metrics read, before models.
        raw['collector_received_at']=time.time()
        raw['collector_received_monotonic_ns']=time.monotonic_ns()

    def record_anchor(self, run, raw):
        anchor = {'profile': PROFILE, 'role': 'raw_anchor_not_a_training_row', 'counters': project(raw)}
        path = self.root / (run + '-cycle-anchor.json')
        with path.open('x', encoding='utf-8') as stream:
            json.dump(anchor, stream, ensure_ascii=False, allow_nan=False)
