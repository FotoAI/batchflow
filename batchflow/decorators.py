import loguru
logger = loguru.logger

import time

def log_time(func):
    def inner(*args ,**kwargs):
        t1 = time.time()
        r = func(*args, **kwargs)
        logger.debug(f"function {func} run time: {time.time()-t1} sec")
        return r
    return inner
