import functools
from typing import Any
import logging

logger = logging.getLogger(__name__)
class Cache():
    _table = dict()

    def __init__(self) -> None:
        self._table = dict()
    
    def __getitem__(self, key):
        try:
            return self._table[key]
        except KeyError:
            raise KeyError(key)
    def __setitem__(self, key, value):
            self._table[key] = value
    
    def __repr__(self) -> str:
        return self._table.__repr__()
    
    def getItem(self, key):
        if key not in self._table.keys():
            return None
        else:
            return self._table[key]

def tuplify(arguments: dict):
    return tuple((k, tuple(v)) for k, v in arguments.items())


def app_reuse(cache, args_to_ignore):
    """Function to be used as decorator to allow the reuse (cache) of some apps functions.

    At least until now, the apps need to contain only named arguments (kwargs)

    Args:
        cache (Cache's class object): Object to store the cache items
        args_to_ignore (list): List containing the arguments to be ignored when caching
    
    Example of use:

    @app_reuse(cache=Cache(), args_to_ignore=["bar"])\\
    def foo(bar):\\
        return "foo" + str(bar)
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper_decorator(*args, **kwargs):
            filtered_kwargs = dict()
            for kwarg in kwargs.keys():
                if kwarg not in args_to_ignore:
                    filtered_kwargs[kwarg] = kwargs[kwarg]
            t_args = tuplify(filtered_kwargs)
            key= hash(t_args)
            try:
                value = cache[key]
                if value:
                    logger.debug("Value is already in the cache")
                return value
            except KeyError:
                ret = func(*args, **kwargs)
                logger.debug("Value is not in the cache. Adding!")
                cache[key] = ret
                return ret
            
        return wrapper_decorator
    return decorator


class CircularList:
    def __init__(self, slots: int) -> None:
        if not slots:
            raise ValueError
        self.list = [None for _ in range(slots)]
        self.index = 0
        self.max_index = len(self.list) - 1

    def next(self) -> Any:
        if self.index == self.max_index:
            self.index = 0
        else:
            self.index += 1
        return self.list[self.index]

    def current(self, value: Any) -> None:
        self.list[self.index] = value
        return
