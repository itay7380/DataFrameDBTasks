import matplotlib.pyplot as plt
import functools
from memory_profiler import memory_usage


def TrackMemory(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        mem_usage = memory_usage(func)
        plt.plot(mem_usage)
        plt.xlabel('Time (seconds)')
        plt.ylabel('Memory Usage (MiB)')
        plt.title('Memory Usage during Callable Execution')
        plt.show(block=False)
        plt.pause(20)
        plt.close()

        return mem_usage

    return wrapper
