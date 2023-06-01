class staticproperty(object):
    """
    Decorator class for creating static properties.

    Notes
    -----
    This class allows creating static properties in a class.
    It behaves like the built-in `property` decorator but for static properties instead of instance properties.
    """

    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


from collections.abc import Collection, Mapping, Hashable
from frozendict import frozendict
import functools

def deep_freeze(thing):
    """
    Recursively freeze a mutable object.

    Parameters
    ----------
    thing : Any
        The object to freeze.

    Returns
    -------
    Any
        The frozen object.

    Notes
    -----
    This function recursively freezes a mutable object by converting it into an immutable equivalent.
    The freezing process ensures that the object and its nested elements cannot be modified.

    Supported types:
    - None and str: Returned as-is since they are immutable.
    - Mapping (e.g., dict): Converted into a `frozendict`, which is an immutable mapping.
    - Collection (e.g., list, tuple, set): Converted into a tuple of frozen elements.
    - Other hashable types: Returned as-is since they are immutable.
    - Other types: Raise a TypeError indicating that the type is not freezable.

    This function is useful for creating immutable versions of objects, which can be beneficial for various purposes such as caching or ensuring data integrity.
    """

    if thing is None or isinstance(thing, str):
        return thing
    elif isinstance(thing, Mapping):
        return frozendict({k: deep_freeze(v) for k, v in thing.items()})
    elif isinstance(thing, Collection):
        return tuple(deep_freeze(i) for i in thing)
    elif not isinstance(thing, Hashable):
        raise TypeError(f"unfreezable type: '{type(thing)}'")
    else:
        return thing

def immutable_arguments(func):
    """
    Decorator that transforms mutable dictionaries into immutable.

    Notes
    -----
    This decorator transforms the mutable dictionaries in the function arguments into their immutable equivalents.
    It is useful to ensure compatibility with caching mechanisms that require immutable inputs.

    Example
    -------
    @immutable_arguments
    def my_function(data):
        # Perform operations with the immutable 'data' argument
    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        return func(*deep_freeze(args), **deep_freeze(kwargs))

    return wrapped
