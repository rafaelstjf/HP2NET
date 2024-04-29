import functools
from typing import Any


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

class _HashedTuple(tuple):
    """A tuple that ensures that hash() will be called no more than once
    per element, since cache decorators will hash the key multiple
    times on a cache miss.  See also _HashedSeq in the standard
    library functools implementation.

    """

    __hashvalue = None

    def __hash__(self, hash=tuple.__hash__):
        hashvalue = self.__hashvalue
        if hashvalue is None:
            self.__hashvalue = hashvalue = hash(self)
        return hashvalue

    def __add__(self, other, add=tuple.__add__):
        return _HashedTuple(add(self, other))

    def __radd__(self, other, add=tuple.__add__):
        return _HashedTuple(add(other, self))

    def __getstate__(self):
        return {}
    def getValue(self):
        return self.__hashvalue


# used for separating keyword arguments; we do not use an object
# instance here so identity is preserved when pickling/unpickling
_kwmark = (_HashedTuple,)


def hashkey(*args, **kwargs):
    """Return a cache key for the specified hashable arguments."""

    kwargs_filtered = dict()
    for k in kwargs.keys():
        if type(kwargs[k]) != dict:
            kwargs_filtered[k] = kwargs[k]
    args_filtered = tuple()
    for arg in args:
        if type(arg) != dict:
            args_filtered.append(arg)
    if kwargs:
        return _HashedTuple(args_filtered + sum(sorted(kwargs_filtered.items()), _kwmark))
    else:
        return _HashedTuple(args_filtered)


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
                    print("Value is already in the cache")
                return value
            except KeyError:
                ret = func(*args, **kwargs)
                print("Value is not in the cache. Adding!")
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
