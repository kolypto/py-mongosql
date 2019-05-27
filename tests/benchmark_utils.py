from time import time_ns
from collections import defaultdict


class Nanotimers:
    """ A timer with nanosecond precision """

    __slots__ = ('_timers', '_results')

    def __init__(self):
        self._timers = {}
        self._results = defaultdict(int)

    def start(self, name):
        self._timers[name] = time_ns()

    def stop(self, name):
        total = time_ns() - self._timers[name]
        self._results[name] += total

    def overhead_shift(self, value):
        for name in self._results:
            self._results[name] -= value

    def __getitem__(self, name):
        return self._results[name] / 10**9

    def dict(self):
        return {name: ns / 10**9 for name, ns in self._results.items()}

    def results(self):
        min_time = min(self._results.values())
        return {
            name: {
                'time': ns / 10**9,
                'perc': 100 * ns / min_time,
            }
            for name, ns in self._results.items()
        }

    def __str__(self):
        return '\n'.join(
            f'{name}: {res["time"]:.02f}s ({res["perc"]:.02f}%)'
            for name, res in self.results().items()
        )


def benchmark_parallel(n_iterations, n_parts, **tests):
    """ Run the given tests in parallel.

    It will switch between all the given tests back and forth, making sure that some local
    performance fluctuations won't hurt running the tests

    Args
    ----

    n_iterations: int
        The total number of iterations
    n_parts: int
        The number of parts to break those iterations into
    tests:
        Named tests to run.
        Each is a callable that receives the `n` argument: number of repetitions
    """
    timers = Nanotimers()
    iters_per_run = n_iterations // n_parts

    # Run
    for run in range(n_parts):
        for name, test in tests.items():
            timers.start(name)
            test(iters_per_run)
            timers.stop(name)

    # Fix overhead
    # Measure overhead
    def f(): pass
    t1 = time_ns()
    for i in range(n_iterations): f()
    t2 = time_ns()
    overhead_ns = t2 - t1
    # Fix it
    timers.overhead_shift(overhead_ns)

    # Done
    return timers


def benchmark_parallel_funcs(n_iterations, n_parts, *funcs):
    return benchmark_parallel(
        n_iterations,
        n_parts,
        **{f.__name__: f for f in funcs})
