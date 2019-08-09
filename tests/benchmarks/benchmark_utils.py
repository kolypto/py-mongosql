from time import time_ns
from collections import defaultdict


class Nanotimers:
    """ A timer with nanosecond precision

    This timer lets you do start() and stop() many times, measuring small intervals.
    It supports measuring many things at once, each having its distinct `name`.

    Because it's designed for benchmarking functions, where a function call itself has overhead in Python,
    it can also compensate for this overhead with overhead_shift().

    Typical usage:

        # Init
        timers = Nanotimers()

        # Measure a `test-name`
        timers.start('test-name')
        for i in range(1000):
            call_your_function()
        timers.stop('test-name')

        timers.overhead_shift(100)  # shift all results by 100ns (as measured by N iterations of an empty function)
    """

    __slots__ = ('_timers', '_results')

    def __init__(self):
        self._timers = {}
        self._results = defaultdict(int)

    def start(self, name):
        """ Start measuring time for `name` """
        self._timers[name] = time_ns()

    def stop(self, name):
        """ Stop measuring time for `name`.

        You can add more time by calling start()/stop() again.
        """
        total = time_ns() - self._timers[name]
        self._results[name] += total

    def overhead_shift(self, value):
        """ Reduce all results by `value` nanoseconds to compensate for some overhead """
        for name in self._results:
            self._results[name] -= value

    def __getitem__(self, name):
        return self._results[name] / 10**9

    def dict(self):
        return {name: ns / 10**9 for name, ns in self._results.items()}

    def results(self):
        """ Get results.

        This method does not only return the raw measured times, but also calculates the relative percentages.
        Example return value:

            {
                'your-name': dict(
                    time=1.2,  # seconds
                    perc=120,  # percent of the best time
                ),
                ...
            }
        """
        min_time = min(self._results.values())
        return {
            name: {
                'time': ns / 10**9,
                'perc': 100 * ns / min_time,
            }
            for name, ns in self._results.items()
        }

    def __str__(self):
        """ Format the measured values to make it look great! """
        return '\n'.join(
            f'{name}: {res["time"]:.02f}s ({res["perc"]:.02f}%)'
            for name, res in self.results().items()
        )


def benchmark_parallel(n_iterations, n_parts, **tests):
    """ Run the given tests in parallel.

    It will switch between all the given tests back and forth, making sure that some local
    performance fluctuations won't hurt running the tests.

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
    # Measure overhead: call an empty function the same number of times
    def f(): pass
    t1 = time_ns()
    for i in range(n_iterations): f()
    t2 = time_ns()
    overhead_ns = t2 - t1
    # Fix it: shift all results by the measured number of nanoseconds
    timers.overhead_shift(overhead_ns)

    # Done
    return timers


def benchmark_parallel_funcs(n_iterations, n_parts, *funcs):
    """ Run the given `funcs` test functions `n_iterations` times.

    Every function receives the `n` argument and is supposed to do its job `n` times in a loop.
    This is to reduce the impact of a repeated function call, and to let your tests initialize before they run.

    Names of those functions are used in displaying results.
    """
    return benchmark_parallel(
        n_iterations,
        n_parts,
        **{f.__name__: f for f in funcs})
