import functools
import os
import time
from functools import wraps

import psutil


def timer(number=10):
    """Decorator that times the function it wraps over repeated executions

    Args:
        number: int, The number of repeated executions of the function being wrapped

    Returns:
        func
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
            print(f"Elapsed time of {func.__name__} for {number} runs:\n" f" {elapsed_time:0.6f} seconds")
            return value

        return wrapper_timer

    return actual_wrapper


# this decorator is used to record memory usage of the decorated function
def record_mem_usage(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())
        mem_start = process.memory_info()[0]
        rt = func(*args, **kwargs)
        mem_end = process.memory_info()[0]
        diff_KB = (mem_end - mem_start) // 1000
        print(f"memory usage of {func.__name__}: {diff_KB} KB")
        return rt

    return wrapper


def record_time_usage(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"Function {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds")
        return result

    return timeit_wrapper
