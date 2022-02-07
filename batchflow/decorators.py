import loguru

logger = loguru.logger

import time


def log_time(func):
    def log_time(*args, **kwargs):
        t1 = time.time()
        r = func(*args, **kwargs)
        logger.log(
            "TIME",
            f"function {func.__module__}:{func.__name__} took run time: {time.time()-t1} sec",
        )
        return r

    return log_time
