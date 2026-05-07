from __future__ import annotations

import functools
import os
import time
from functools import wraps

# psutil is an optional dev-only dep used solely by `record_mem_usage`. Keep
# the import lazy so module load (and sphinx autodoc) doesn't require it;
# the decorator will raise a clear ImportError only if someone actually
# tries to use it without psutil installed.
try:
    import psutil
except ImportError:  # pragma: no cover
    psutil = None  # type: ignore[assignment]


def timer(number=10):
    """Decorator that times the function it wraps over repeated executions.

    Calls the wrapped function ``number`` times back-to-back and prints
    the cumulative wall-clock duration. Returns the value of the final
    call so the decorated function still behaves like the original from
    the caller's perspective.

    Args:
        number: Number of repeated executions of the wrapped function.
            Defaults to ``10``.

    Returns:
        A decorator that wraps a callable with timing instrumentation.

    Example:
        Time a small loader 5 times::

            from sportsdataverse.decorators import timer

            @timer(number=5)
            def warmup():
                return sum(range(1_000_000))

            warmup()
            # Elapsed time of warmup for 5 runs:
            #  0.123456 seconds
    """

    def actual_wrapper(func):
        @functools.wraps(func)
        def wrapper_timer(*args, **kwargs):
            tic = time.perf_counter()
            for i in range(number - 1):
                func(*args, **kwargs)
            else:
                value = func(*args, **kwargs)
            toc = time.perf_counter()
            elapsed_time = toc - tic
            print(f"Elapsed time of {func.__name__} for {number} runs:\n {elapsed_time:0.6f} seconds")
            return value

        return wrapper_timer

    return actual_wrapper


# this decorator is used to record memory usage of the decorated function
def record_mem_usage(func):
    """Decorator that prints RSS memory delta around a call.

    Snapshots ``psutil.Process(os.getpid()).memory_info()[0]`` (resident
    set size) before and after the wrapped call and prints the
    kilobytes consumed. Requires the optional ``psutil`` extra; without
    it the decorator raises ``ImportError`` when invoked.

    Args:
        func: Callable to wrap.

    Returns:
        The wrapped callable.

    Example:
        Track memory growth of a parquet load::

            from sportsdataverse.decorators import record_mem_usage
            from sportsdataverse.nfl import load_nfl_pbp

            @record_mem_usage
            def load_one():
                return load_nfl_pbp(seasons=[2024])

            df = load_one()
            # memory usage of load_one: 124800 KB
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        if psutil is None:
            raise ImportError(
                "record_mem_usage requires `psutil`; install it via `uv add --dev psutil` or `pip install psutil`.",
            )
        process = psutil.Process(os.getpid())
        mem_start = process.memory_info()[0]
        rt = func(*args, **kwargs)
        mem_end = process.memory_info()[0]
        diff_KB = (mem_end - mem_start) // 1000
        print(f"memory usage of {func.__name__}: {diff_KB} KB")
        return rt

    return wrapper


def record_time_usage(func):
    """Decorator that prints wall-clock time for a single call.

    Lightweight counterpart to :func:`timer` -- records exactly one
    invocation rather than averaging across repeated executions. Useful
    when sprinkled over loaders during ad-hoc profiling.

    Args:
        func: Callable to wrap.

    Returns:
        The wrapped callable.

    Example:
        Time a single ESPN scoreboard call::

            from sportsdataverse.decorators import record_time_usage
            from sportsdataverse.cfb import espn_cfb_scoreboard

            @record_time_usage
            def grab():
                return espn_cfb_scoreboard(dates=20240907)

            scores = grab()
            # Function grab() {} Took 0.4231 seconds
    """

    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"Function {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds")
        return result

    return timeit_wrapper
