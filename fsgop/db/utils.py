from typing import Iterable
import re
from datetime import date, datetime
from itertools import chain

DATE_FORMAT = "%Y-%m-%d"

Pattern = type(re.compile('', 0))

REPLACEMENTS = {
    "ä": "ae",
    "Ä": "Ae",
    "ö": "oe",
    "Ö": "Oe",
    "ü": "ue",
    "Ü": "Ue",
    "'": ""
}
ASCII = str.maketrans(REPLACEMENTS)


def from_str(string, cls):
    if issubclass(cls, str):
        return string
    if issubclass(cls, date):
        return datetime.strptime(string, DATE_FORMAT).date()
    return cls(string)


def to(cls, obj, **kwargs):
    """Helper to create instances of a given class

    Arguments:
        cls (type or callable): Class of instance to create, e.g. ``str``
        obj (object): Object containing source information. If this is an
            instance of class `cls`, then it is returned. If this is a
            :class:`collections.namedtuple`, then it will be forwared as keyword
             arguments to the constructor of cls. If `obj` is ``None`` and
             `default` is provided, then `default` is returned.
        default: Default value.

    Return:
          obj: Instance of `cls` or ``None``
    """
    try:
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, str):
            return from_str(obj, cls)
    except TypeError:
        # cls is not a class -> assume it is callable
        pass
    if isinstance(obj, dict):
        return cls(**obj)
    if isinstance(obj, tuple):
        try:
            # works for named tuples only
            return cls(**obj._asdict())
        except AttributeError:
            return cls(*obj)
    if obj is None:
        try:
            return kwargs['default']
        except KeyError:
            raise ValueError("Value may not be <None>")
    return cls(obj)


def get_key_value_pattern(key):
    """Get regular expression pattern for key value pairs in comments

    Arguments:
        key(str): Key to search for

    Return:
        re.Pattern: Regular expression pattern matching key
    """
    return re.compile("".join([f"({key})", r"\s*[=:]\s*'(.+)'"]))


def iter_attrs(cls, ignore=None):
    """Iterate over all members of a class

    Arguments:
       cls (object): Class instance over which to iterate
       ignore (iterable): Iterable of members to ignore

    Yield:
        Tuple containing name and value of each attribute of *cls*.
    """
    skip = set(ignore) if ignore is not None else set()
    for key, val in vars(cls).items():
        if key.startswith('__') or key in skip:
            continue
        yield key, val


def copy_attrs(src, dest, ignore=None):
    """Copy all members of src to dest

    Arguments:
        src(object): Source object
        dest(object): Destination object
        ignore(iterable): Iterable of members to ignore during copy. Defaults to
           ``None``.
    """
    for name, val in iter_attrs(src, ignore):
        setattr(dest, name, val)


def all_attrs_equal(cls1, cls2, ignore=None):
    """Check if two class instances have equal members

    Arguments:
        cls1(object): First object
        cls2(object): Second object
        ignore(iterable): Iterable of members to ignore in comparison. Defaults
            to ``None``.

    Return
        bool: ``True`` if and only if all not ignored members in *cls1* and *cls2*
        compare equal.
    """
    for name, val in iter_attrs(cls1, ignore):
        try:
            if val != getattr(cls2, name):
                return False
        except AttributeError:
            return False
    return True


def find_key_value_pair(s, key):
    if not s:
        return None

    pattern = key if isinstance(key, Pattern) else get_key_value_pattern(key)
    return pattern.search(s)


def get_value(s, key):
    """Gets field from comment.

    Comment fields are strings of the format '``key`` = ``value``'

    Arguments:
        s (str): string to search for key value pattern
        key (str or re.Pattern): Name of key to retrieve or regular
            expression Pattern instance.

    Return:
        str: value associated with *key* or ``None`` if *key* does not exist.
    """
    match = find_key_value_pair(s, key)
    if not match:
        return None
    return match.group(2)


def set_value(key, value, s=""):
    """Set key value pair value.

    Comment fields are strings of the format '``key`` = ``value``'

    Arguments:
        key (str): Name of key to set
        value (str): Value string. If ``None``, the key value pair is
           removed, if it exists.
        s (str): String with additional key value pairs
    """
    if not key:
        raise ValueError("Key must not be empty")

    if not s:
        return f"{key}='{value}'"

    match = find_key_value_pair(s, key)
    if match:
        # key exists -> replace
        return "".join([s[0:match.start(2)], f"{value}", s[match.end(2):]])
    else:
        return "; ".join([s, f"{key}='{value}'"])


def kwargs_from(obj: object, layout: dict) -> dict:
    """Extract nested keyword arguments from object based on a given structure

    Args:
        obj: An object with flattened arguments
        layout: Output structure. Keys in this dict will be keys in the
            output dictionary. The associated value will either be extracted
            using :func:`getattr` if the value is a string or by recursive
            invocation of :func:`kwargs_from`.

    Returns:
         Possibly nested dictionary of keyword arguments
    """
    return {
        k: getattr(obj, v) if isinstance(v, str) else kwargs_from(obj, v)
        for k, v in layout.items()
    }


class Sequence(object):
    """Analyse sequence objects

    Allows to analyse the first element of a sequence and to iterate over the
    entire sequence including the first element

    Args:
        seq: Sequence to analyse and wrap
    """
    def __init__(self, seq: Iterable):
        self._iter = iter(seq)
        self._first = None

        for x in self._iter:
            self._first = x
            break

    def __iter__(self) -> Iterable:
        if self._first is not None:
            yield from chain((self._first,), self._iter)
        else:
            yield from self._iter

    def __bool__(self):
        return self._first is not None

    @property
    def element_type(self):
        """Get type of first element in sequence"""
        if self._first is None:
            return None
        return type(self._first)
