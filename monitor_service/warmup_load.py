"""A fixed normal-load curriculum for the owned application, not a new PCA.

Each fit/reference/calibration part keeps its old length. No fault controls,
outcome-driven rate selection, resets or shortened training are introduced.
"""


class NormalLoadPlan:
    def __init__(self, slot_seconds=64, slots=14, rates=(2, 8)):
        if slot_seconds != 64 or slots != 14 or tuple(rates) != (2, 8):
            raise ValueError('This version supports only the fixed 14 x 64-second plan.')
        self.slot_seconds, self.slots, self.rates = slot_seconds, slots, tuple(rates)
        self.duration = slot_seconds * slots

    def at(self, elapsed):
        if elapsed < 0:
            raise ValueError('Elapsed time must be nonnegative.')
        index = min(int(elapsed // self.slot_seconds), self.slots)
        return dict(index=index, planned_start=index*self.slot_seconds,
                    rate=self.rates[index % 2] if index < self.slots else 2,
                    curriculum_complete=index == self.slots)

    def describe(self):
        return [dict(index=i, start_seconds=i*self.slot_seconds,
                     end_seconds=(i+1)*self.slot_seconds, rate=self.rates[i % 2])
                for i in range(self.slots)]
