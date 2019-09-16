import inspect
from functools import lru_cache
from typing import Callable, Mapping, Tuple


@lru_cache(100)
def get_function_defaults(for_func: Callable) -> dict:
    """ Get a dict of function's arguments that have default values """
    # Analyze the method
    argspec = inspect.getfullargspec(for_func)  # TODO: use signature(): Python 3.3

    # Get the names of the kwargs
    # Only process those that have defaults
    n_args = len(argspec.args) - len(argspec.defaults or ())  # Args without defaults
    kwargs_names = argspec.args[n_args:]

    # Get defaults for kwargs: put together argument names + default values
    defaults = dict(zip(kwargs_names, argspec.defaults or ()))

    # Done
    return defaults


def pluck_kwargs_from(dct: Mapping, for_func: Callable, skip: Tuple[str] = ()) -> dict:
    """ Analyze a function, pluck the arguments it needs from a dict """
    defaults = get_function_defaults(for_func)

    # Get the values for these kwargs
    kwargs = {k: dct.get(k, defaults[k])
              for k in defaults.keys()
              if k not in skip}

    # Done
    return kwargs
