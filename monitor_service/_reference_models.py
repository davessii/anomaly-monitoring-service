"""Small NumPy models for a reproducible pilot; no test labels enter training."""
from dataclasses import dataclass
import copy
import numpy as np


@dataclass(frozen=True)
class Config:
    components: int = 5
    batch_size: int = 128
    forgetting: float = 0.85
    excluded_fraction: float = 0.10
    random_repeats: int = 7
    guard_ratio: float = 3.0
    min_model_distance: float = 0.025
    min_threshold_change: float = 0.10
    threshold_quantile: float = 0.99
    history_size: int = 512
    ae_initial_epochs: int = 250
    ae_update_epochs: int = 5


class IncrementalSubspace:
    """Incremental SVD with mean correction and optional exponential forgetting.

    At forgetting=1 this is cumulative truncated incremental PCA. For f<1,
    the previous scatter matrix and sample mass are multiplied by f per update.
    This is a research implementation, not sklearn's IncrementalPCA class.
    """
    def __init__(self, components, forgetting=1.0):
        self.k = components
        self.forgetting = forgetting
        self.mass = 0.0
        self.mean = None
        self.components = None
        self.singular = None

    def update(self, x):
        x = np.asarray(x, dtype=float)
        if x.ndim != 2 or len(x) < self.k or self.k > x.shape[1]:
            raise ValueError("Each update needs at least k rows, and k <= features")
        if not np.isfinite(x).all():
            raise ValueError("Data must contain finite numbers")
        batch_mean = x.mean(axis=0)
        centered = x - batch_mean
        if self.mean is None:
            new_mean, mass = batch_mean, float(len(x))
            matrix = centered
        else:
            old_mass = self.mass * self.forgetting
            mass = old_mass + len(x)
            new_mean = (old_mass * self.mean + len(x) * batch_mean) / mass
            correction = np.sqrt(old_mass * len(x) / mass) * (self.mean - batch_mean)
            matrix = np.vstack((
                np.sqrt(self.forgetting) * self.singular[:, None] * self.components,
                centered,
                correction[None, :],
            ))
        _, singular, basis = np.linalg.svd(matrix, full_matrices=False)
        self.mass = mass
        self.mean = new_mean.copy()
        self.singular = singular[:self.k].copy()
        self.components = basis[:self.k].copy()
        return self

    def reconstruct(self, x):
        return ((x - self.mean) @ self.components.T) @ self.components + self.mean

    def score(self, x):
        return np.mean((x - self.reconstruct(x)) ** 2, axis=1)

    def distance(self, other):
        # Projector distance is invariant to eigenvector signs and rotations.
        p = self.components.T @ self.components
        q = other.components.T @ other.components
        return float(np.linalg.norm(p - q) / np.sqrt(2 * self.k)
                     + np.linalg.norm(self.mean - other.mean) / np.sqrt(len(self.mean)))


class Autoencoder:
    """Tanh bottleneck and linear output, trained with Adam on reconstruction MSE."""
    def __init__(self, dimensions, bottleneck, seed):
        rng = np.random.default_rng(seed)
        self.params = {
            "w1": rng.normal(0, 0.10, (dimensions, bottleneck)),
            "b1": np.zeros(bottleneck),
            "w2": rng.normal(0, 0.10, (bottleneck, dimensions)),
            "b2": np.zeros(dimensions),
        }
        self.first = {k: np.zeros_like(v) for k, v in self.params.items()}
        self.second = {k: np.zeros_like(v) for k, v in self.params.items()}
        self.step = 0

    def reconstruct(self, x):
        p = self.params
        hidden = np.tanh(x @ p["w1"] + p["b1"])
        return hidden @ p["w2"] + p["b2"]

    def score(self, x):
        return np.mean((x - self.reconstruct(x)) ** 2, axis=1)

    def loss_gradients(self, x):
        p = self.params
        hidden = np.tanh(x @ p["w1"] + p["b1"])
        reconstruction = hidden @ p["w2"] + p["b2"]
        residual = reconstruction - x
        dy = 2 * residual / residual.size
        dh = (dy @ p["w2"].T) * (1 - hidden ** 2)
        gradients = {"w1": x.T @ dh, "b1": dh.sum(axis=0),
                     "w2": hidden.T @ dy, "b2": dy.sum(axis=0)}
        return float(np.mean(residual ** 2)), gradients

    def update(self, x, epochs=5, learning_rate=0.002):
        for _ in range(epochs):
            _, gradients = self.loss_gradients(x)
            self.step += 1
            for key, grad in gradients.items():
                grad = np.clip(grad, -5, 5)
                self.first[key] = 0.9 * self.first[key] + 0.1 * grad
                self.second[key] = 0.999 * self.second[key] + 0.001 * grad ** 2
                m = self.first[key] / (1 - 0.9 ** self.step)
                v = self.second[key] / (1 - 0.999 ** self.step)
                self.params[key] -= learning_rate * m / (np.sqrt(v) + 1e-8)
        return self


MODES = {
    "fixed": "Фиксированная PCA",
    "cumulative": "Накопительная IPCA",
    "adaptive": "IPCA с забыванием",
    "trimmed": "IPCA с исключением точек",
    "guarded": "IPCA с проверкой обновления",
    "autoencoder": "Дообучаемый автоэнкодер",
}


class Detector:
    """Predict first, then update. train/calibration are separate time periods.

    The guarded mode independently checks model updates and admission to the
    threshold reference buffer. Threshold comparisons use one frozen model.
    On the next step the accepted reference buffer is rescored by the current
    model. Thus a rejected buffer update does not freeze the numerical threshold
    when the reconstruction model itself changes.
    """
    def __init__(self, mode, train, calibration, config=Config(), seed=41):
        if mode not in MODES:
            raise ValueError("Unknown mode")
        train, calibration = np.asarray(train, dtype=float), np.asarray(calibration, dtype=float)
        if train.ndim != 2 or calibration.ndim != 2 or train.shape[1] != calibration.shape[1]:
            raise ValueError("Training and calibration must have the same feature columns")
        if not 1 <= config.components < train.shape[1]:
            raise ValueError("For anomaly detection choose 1 <= components < feature count")
        if min(len(train), len(calibration)) < config.components or not np.isfinite(train).all() or not np.isfinite(calibration).all():
            raise ValueError("Training and calibration need enough finite numerical rows")
        if not 0 < config.forgetting <= 1 or not 0 < config.threshold_quantile < 1:
            raise ValueError("Invalid forgetting or threshold quantile")
        self.mode, self.config = mode, config
        self.rng = np.random.default_rng(seed)
        self.center = train.mean(axis=0)
        self.scale = np.maximum(train.std(axis=0), 1e-8)
        train = self.normalize(train)
        if mode == "autoencoder":
            self.model = Autoencoder(train.shape[1], config.components, seed)
            self.model.update(train, config.ae_initial_epochs, 0.01)
        else:
            f = 1.0 if mode in ("fixed", "cumulative") else config.forgetting
            self.model = IncrementalSubspace(config.components, f)
            if mode == "fixed":
                self.model.update(train)
            else:
                for batch in np.array_split(train, max(1, len(train) // config.batch_size)):
                    self.model.update(batch)
        self.history = self.normalize(calibration)[-config.history_size:].copy()

    def normalize(self, x):
        x = np.asarray(x, dtype=float)
        if x.ndim != 2 or x.shape[1] != len(self.center) or not np.isfinite(x).all():
            raise ValueError("New data must be finite and use the same feature columns")
        return (x - self.center) / self.scale

    def _history(self, x):
        return np.vstack((self.history, x))[-self.config.history_size:]

    def threshold(self):
        return max(float(np.quantile(self.model.score(self.history),
                                     self.config.threshold_quantile)), 1e-12)

    def process(self, raw):
        x = self.normalize(raw)
        if len(x) < self.config.components:
            raise ValueError("Batch is too short")
        c = self.config
        score, threshold = self.model.score(x), self.threshold()
        prediction = score > threshold
        log = {"model_decision": "fixed", "history_decision": "fixed",
               "model_ratio": None, "threshold_ratio": None}
        if self.mode == "fixed":
            return score, prediction, threshold, log
        removed = max(1, min(int(np.ceil(len(x) * c.excluded_fraction)), len(x) - c.components))
        suspicious = np.argsort(score)[-removed:]
        keep = np.ones(len(x), dtype=bool)
        keep[suspicious] = False
        if self.mode == "guarded":
            full = copy.deepcopy(self.model).update(x)
            trimmed = copy.deepcopy(self.model).update(x[keep])
            model_change = full.distance(trimmed)
            # Same frozen model for every threshold candidate; raw buffer truncation
            # is identical to the actual admission rule.
            full_threshold = float(np.quantile(self.model.score(self._history(x)), c.threshold_quantile))
            clean_threshold = float(np.quantile(self.model.score(self._history(x[keep])), c.threshold_quantile))
            threshold_change = abs(full_threshold - clean_threshold) / max(threshold, 1e-12)
            random_model, random_threshold = [], []
            for _ in range(c.random_repeats):
                random_keep = np.ones(len(x), dtype=bool)
                random_keep[self.rng.choice(len(x), removed, replace=False)] = False
                variant = copy.deepcopy(self.model).update(x[random_keep])
                random_model.append(full.distance(variant))
                qt = float(np.quantile(self.model.score(self._history(x[random_keep])), c.threshold_quantile))
                random_threshold.append(abs(full_threshold - qt) / max(threshold, 1e-12))
            model_ratio = model_change / max(float(np.median(random_model)), 1e-8)
            threshold_ratio = threshold_change / max(float(np.median(random_threshold)), 1e-8)
            model_reject = model_ratio > c.guard_ratio and model_change > c.min_model_distance
            history_reject = threshold_ratio > c.guard_ratio and threshold_change > c.min_threshold_change
            if not model_reject:
                self.model = full
            if not history_reject:
                self.history = self._history(x)
            log = {"model_decision": "rejected" if model_reject else "accepted",
                   "history_decision": "rejected" if history_reject else "accepted",
                   "model_ratio": float(model_ratio), "threshold_ratio": float(threshold_ratio)}
        else:
            admitted = x[keep] if self.mode == "trimmed" else x
            if self.mode == "autoencoder":
                self.model.update(admitted, c.ae_update_epochs)
            else:
                self.model.update(admitted)
            self.history = self._history(admitted)
            log.update(model_decision="trimmed" if self.mode == "trimmed" else "accepted",
                       history_decision="trimmed" if self.mode == "trimmed" else "accepted")
        return score, prediction, threshold, log

    def state_bytes(self):
        arrays = [self.center, self.scale, self.history]
        if isinstance(self.model, IncrementalSubspace):
            arrays += [self.model.mean, self.model.components, self.model.singular]
        else:
            arrays += list(self.model.params.values()) + list(self.model.first.values()) + list(self.model.second.values())
        return sum(a.nbytes for a in arrays)

    def save(self, path):
        state = {"center": self.center, "scale": self.scale, "history": self.history,
                 "mode": np.array(self.mode)}
        if isinstance(self.model, IncrementalSubspace):
            state.update(mean=self.model.mean, components=self.model.components,
                         singular=self.model.singular, mass=np.array(self.model.mass))
        else:
            state.update(self.model.params)
            state.update({"adam_first_" + k: v for k, v in self.model.first.items()})
            state.update({"adam_second_" + k: v for k, v in self.model.second.items()})
            state["adam_step"] = np.array(self.model.step)
        np.savez_compressed(path, **state)
