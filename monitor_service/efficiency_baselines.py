"""Causal simple comparisons for the development pilot; no outcome inputs.

This module does not open databases, start workloads or evaluate event labels.
The existing IPCA procedure is not modified. Missing scores are not normality.
"""
from collections import deque
from bisect import bisect_right
import math
import time
import numpy as np

METHODS = ('separate_limits', 'cpu_per_request', 'expected_cpu')
FEATURES = ('app_cpu_percent', 'app_memory_mib', 'active_requests', 'completed_per_second')
FIT, REFERENCE, CALIBRATION, BLOCK = 256, 128, 512, 16
WARMUP = FIT + REFERENCE + CALIBRATION
RAW_FIELDS = ('timestamp', 'cpu_seconds', 'requests_completed', 'memory_mib', 'active_requests')


def project_counters(raw):
    """Whitelist numeric measurement inputs. Control/outcome fields are ignored."""
    clean = {}
    for key in RAW_FIELDS:
        value = raw[key]
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ValueError('Invalid measurement: ' + key)
        if value < 0:
            raise ValueError('Negative measurement: ' + key)
        clean[key] = float(value)
    if clean['timestamp'] <= 0:
        raise ValueError('Timestamp must be positive.')
    for key in ('requests_completed', 'active_requests'):
        if not clean[key].is_integer():
            raise ValueError('Counter must be an integer: ' + key)
    return clean


class CausalWindow:
    """Use actual endpoints 10..13s apart; never interpolate or fill a gap."""
    def __init__(self):
        self.rows = deque()

    def observe(self, raw):
        row = project_counters(raw)
        if self.rows and row['timestamp'] <= self.rows[-1]['timestamp']:
            raise ValueError('Measurements must be strictly chronological.')
        self.rows.append(row)
        # Keep the latest endpoint preceding t-10 and the entire recent window.
        while len(self.rows) > 1 and self.rows[1]['timestamp'] <= row['timestamp'] - 13:
            self.rows.popleft()
        rows = list(self.rows)
        index = bisect_right([r['timestamp'] for r in rows], row['timestamp'] - 10) - 1
        if index < 0:
            return {'timestamp': row['timestamp'], 'valid': False, 'reason': 'window_not_ready'}
        part = rows[index:]
        duration = part[-1]['timestamp'] - part[0]['timestamp']
        reason = None
        if duration > 13:
            reason = 'endpoint_too_old'
        elif any(b['timestamp'] - a['timestamp'] > 3 for a, b in zip(part, part[1:])):
            reason = 'measurement_gap'
        elif any(b[k] < a[k] for a, b in zip(part, part[1:])
                 for k in ('cpu_seconds', 'requests_completed')):
            reason = 'counter_reset'
        if reason:
            return {'timestamp': row['timestamp'], 'valid': False, 'reason': reason}
        elapsed = [b['timestamp'] - a['timestamp'] for a, b in zip(part, part[1:])]
        # Right-endpoint time weighting; no claim of exact continuous-time mean.
        mean = lambda key: sum(dt * r[key] for dt, r in zip(elapsed, part[1:])) / duration
        cpu_ms = (part[-1]['cpu_seconds'] - part[0]['cpu_seconds']) * 1000
        completed = part[-1]['requests_completed'] - part[0]['requests_completed']
        vector = [cpu_ms / duration / 10, mean('memory_mib'), mean('active_requests'), completed / duration]
        if not all(math.isfinite(x) for x in (*vector, cpu_ms)):
            raise ValueError('Non-finite aggregate.')
        return {'timestamp': row['timestamp'], 'window_start': part[0]['timestamp'],
                'duration_seconds': duration, 'valid': True, 'reason': None,
                'cpu_ms': cpu_ms, 'completed': int(completed), 'features': vector,
                'ratio_eligible': completed >= 4}


def upper_cut(values):
    if len(values) < 32:
        raise ValueError('At least 32 eligible scores are required.')
    return float(np.quantile(values, .99, method='higher'))


class SimpleComparisons:
    """Fit 256, check 128, calibrate 512; score before each 16-row cut update."""
    def __init__(self):
        self.window = CausalWindow()
        self.seen = 0
        self.fit_windows = []
        self.histories = {m: deque(maxlen=512) for m in METHODS}
        self.cuts = {}
        self.pending = []
        self.fit_diagnostics = None

    def fit(self):
        valid = [w for w in self.fit_windows if w['valid']]
        if len(valid) < 32:
            raise ValueError('Insufficient valid windows for fitting.')
        matrix = np.asarray([w['features'] for w in valid], float)
        self.center = matrix.mean(axis=0)
        std = matrix.std(axis=0)
        self.scale = np.where(std < 1e-8, 1., std)
        n = np.asarray([w['completed'] for w in valid], float)
        cpu = np.asarray([w['cpu_ms'] for w in valid], float)
        if len(np.unique(n)) < 2:
            raise ValueError('Distinct completed counts are required for expected CPU.')
        design = np.column_stack((np.ones(len(n)), n))
        self.coefficients, _, rank, _ = np.linalg.lstsq(design, cpu, rcond=None)
        if rank != 2 or not np.isfinite(self.coefficients).all():
            raise ValueError('Expected CPU fit is not identifiable.')
        self.n_range = (float(n.min()), float(n.max()))
        self.fit_diagnostics = {'valid_windows': len(valid), 'center': self.center.tolist(),
                                'scale': self.scale.tolist(), 'cpu_intercept_ms': float(self.coefficients[0]),
                                'cpu_slope_ms_per_completion': float(self.coefficients[1]),
                                'completed_range': list(self.n_range)}
        self.fit_windows = []

    def scores(self, window):
        scores = {m: None for m in METHODS}
        elapsed = {m: 0. for m in METHODS}
        extrapolation = None
        if not window['valid']:
            return scores, elapsed, extrapolation
        started = time.perf_counter()
        scores['separate_limits'] = float(np.max(np.abs((np.asarray(window['features']) - self.center) / self.scale)))
        elapsed['separate_limits'] = time.perf_counter() - started
        started = time.perf_counter()
        if window['ratio_eligible']:
            scores['cpu_per_request'] = window['cpu_ms'] / window['completed']
        elapsed['cpu_per_request'] = time.perf_counter() - started
        started = time.perf_counter()
        prediction = float(self.coefficients[0] + self.coefficients[1] * window['completed'])
        scores['expected_cpu'] = max(0., window['cpu_ms'] - prediction)
        extrapolation = not self.n_range[0] <= window['completed'] <= self.n_range[1]
        elapsed['expected_cpu'] = time.perf_counter() - started
        if any(s is not None and not math.isfinite(s) for s in scores.values()):
            raise ValueError('Non-finite score.')
        return scores, elapsed, extrapolation

    def observe(self, counters):
        started = time.perf_counter()
        window = self.window.observe(counters)
        preparation = time.perf_counter() - started
        self.seen += 1
        if self.seen <= FIT:
            self.fit_windows.append(window)
            if self.seen == FIT:
                self.fit()
            return None
        scores, elapsed, extrapolation = self.scores(window)
        if self.seen <= WARMUP:
            if self.seen > FIT + REFERENCE:
                for name, score in scores.items():
                    if score is not None:
                        self.histories[name].append(score)
            if self.seen == WARMUP:
                self.cuts = {m: upper_cut(list(self.histories[m])) for m in METHODS}
            return None
        decisions = {m: {'score': scores[m], 'threshold': self.cuts[m],
                         'alarm': None if scores[m] is None else bool(scores[m] > self.cuts[m]),
                         'own_score_seconds': elapsed[m]} for m in METHODS}
        self.pending.append(scores)
        update_times = {m: 0. for m in METHODS}
        # The current row and entire current block use the previous cuts.
        if len(self.pending) == BLOCK:
            for name in METHODS:
                mark = time.perf_counter()
                self.histories[name].extend(s[name] for s in self.pending if s[name] is not None)
                if len(self.histories[name]) >= 32:
                    self.cuts[name] = upper_cut(list(self.histories[name]))
                update_times[name] = time.perf_counter() - mark
            self.pending = []
        return {'timestamp': window['timestamp'], 'window': window,
                'expected_cpu_extrapolation': extrapolation, 'decisions': decisions,
                'common_preparation_seconds': preparation, 'own_update_seconds': update_times,
                'timing_scope': 'Preparation once, each own score and post-decision cut update separately; not full services.'}


def measurement_summary(counters):
    """Raw measurement gate only. No labels, alarms or detection metrics."""
    builder = CausalWindow()
    ready, eligible, nonzero, invalid, resets = 0, 0, 0, {}, 0
    for raw in counters:
        window = builder.observe(raw)
        if window.get('reason') == 'window_not_ready':
            continue
        ready += 1
        if window['valid'] and window['ratio_eligible']:
            eligible += 1
            nonzero += window['cpu_ms'] > 0
        else:
            reason = window.get('reason') or 'too_few_completions'
            invalid[reason] = invalid.get(reason, 0) + 1
            resets += reason == 'counter_reset'
    fraction = eligible / ready if ready else None
    return {'candidate_windows': ready, 'eligible_windows': eligible,
            'eligible_fraction': fraction, 'positive_cpu_windows': nonzero,
            'invalid_reasons': invalid, 'counter_reset_windows': resets,
            'coverage_gate_passed': fraction is not None and fraction >= .9 and resets == 0,
            'scope': 'Whole supplied segment. Apply to each ordinary segment separately; event/load/outcome checks remain required.'}
