from typing import Iterable, Optional, Union
import re
from datetime import date, datetime
from itertools import chain, islice

DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H:%M:%S"
DATE_TIME_FORMAT = " ".join((DATE_FORMAT, TIME_FORMAT))

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


def from_str(string, cls, **kwargs):
    if issubclass(cls, str):
        return string
    if issubclass(cls, datetime):
        return datetime.strptime(string,
                                 kwargs.get("datetime_fmt", DATE_TIME_FORMAT))
    if issubclass(cls, date):
        return datetime.strptime(string,
                                 kwargs.get("date_fmt", DATE_FORMAT)).date()
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
            return from_str(obj, cls, **kwargs)
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


def get_key_value_pattern(key: str) -> Pattern:
    """Get regular expression pattern for key value pairs in comments

    Arguments:
        key(str): Key to search for

    Return:
        re.Pattern: Regular expression pattern matching key
    """
    return re.compile("".join([f"({key})", r"\s*[=:]\s*'([^']+)';?"]))


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


def find_key_value_pair(s: str,
                        key: Union[str, Pattern]) -> Optional[object]:
    """Find key value pair in string

    Args:
        s: String to search
        key: Either a regular expression pattern or a key name

    Returns:
        Match object if key is found, else ``None``
    """
    if not s:
        return None

    pattern = key if isinstance(key, Pattern) else get_key_value_pattern(key)
    return pattern.search(s)


def get_value(s: str, key: Union[str, Pattern]) -> Optional[str]:
    """Gets field from comment.

    Comment fields are strings of the format '``key`` = ``value``'

    Args:
        s: string to search for key value pattern
        key: Name of key to retrieve or regular expression Pattern instance.

    Return:
        value associated with *key* or ``None`` if *key* does not exist.
    """
    match = find_key_value_pair(s, key)
    if not match:
        return None
    return match.group(2)


def set_value(key: str, value: Optional[str], s: str = "") -> str:
    """Set key value pair value.

    Comment fields are strings of the format '``key`` = ``value``'

    Args:
        key: Name of key to set
        value: Value string. If ``None``, the key value pair is removed,
            if it exists.
        s: String with additional key value pairs

    Returns:
        String with updated key value pair
    """
    if not key:
        raise ValueError("Key must not be empty")

    if not s:
        return f"{key}='{value}'" if value is not None else ""

    match = find_key_value_pair(s, key)
    if match:
        # key exists -> replace
        if value is not None:
            return "".join([s[0:match.start(2)], f"{value}", s[match.end(2):]])
        else:
            return "".join([s[0:match.start()], s[match.end():]]).strip()
    else:
        return "; ".join([s, f"{key}='{value}'"]) if value is not None else s


def kwargs_from(obj: object, layout: dict, *args) -> dict:
    """Extract nested keyword arguments from object based on a given structure

    Args:
        obj: An object with flattened arguments
        layout: Output structure. Keys in this dict will be keys in the
            output dictionary. The associated value will either be extracted
            using :func:`getattr` if the value is a string or by recursive
            invocation of :func:`kwargs_from`.
        *args: Passed verbatim to getattr. Only use is to specify a default
            value to return if an argument is not found.

    Raises:
        AttributeError if an attribute is not found and no default is specified.

    Returns:
         Possibly nested dictionary of keyword arguments
    """
    return {
        k: getattr(obj, v, *args)
        if isinstance(v, str) else kwargs_from(obj, v, *args)
        for k, v in layout.items()
    }


def chunk(seq: Iterable, n: int = 1) -> Iterable:
    """Read sequence in chunks of a given size

    Args:
        seq: Input sequence to read in chunks
        n: Chunk size. If chunk size is less than one, the entire sequence will
           be returned in a single chunk. If n is one, then individual elements
           of the sequence will be returned one by one. Otherwise elements will
           be returned in chunks of size `n`.

    Yields:
        Chunks of size 'n' or less (if end of the sequence is reached)
    """
    if n < 1:
        yield seq
    elif n == 1:
        yield from seq
    else:
        it = iter(seq)
        for first in it:
            yield chain((first,), islice(it, n - 1))


class Sequence(object):
    """Analyse sequence objects

    Allows to analyse the first element of a sequence and to iterate over the
    entire sequence including the first element

    Args:
        seq: Sequence to analyse and wrap
    """
    def __init__(self, seq: Iterable):
        self._iter = iter(seq)
        self._first = next(self._iter, None)

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

