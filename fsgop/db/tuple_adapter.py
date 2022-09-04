from typing import Optional, Type, Iterable, Tuple
from collections import namedtuple
from .utils import Sequence


class AdapterBase(object):
    """Modifies each tuple in a sequence of tuples

    Args:
        rectype: Type of the tuples forming the input sequence. Each tuple in
            the input sequence must be of this type.
    """
    def __init__(self, rectype: Type[namedtuple]) -> None:
        self._copy = []  # list of fields copied from input tuples
        self._add = []   # list of additional fields created by this class
        self._rectype = rectype
        self.set_fields(rectype)

    def __call__(self, t: namedtuple) -> namedtuple:
        """Convert input tuple to output tuple

        Args:
            t: Input tuple containing one or more combined name fields

        Returns:
            named tuple with combined fields replaced by first name and last name
            fields.
        """
        return self._rectype._make(self.iter_args(t))

    def __bool__(self) -> bool:
        """Check if any fields are added by this functor

        Returns:
            ``True`` if and only if this functor does anything useful

        """
        return bool(self._add)

    @property
    def record_type(self) -> Type[namedtuple]:
        """Get record type returned by this functor"""
        return self._rectype

    def set_fields(self, rectype: Type[namedtuple]) -> None:
        """Set fields to copy or add for a given record type

        Has to be implemented by the derived class.

        Args:
            rectype: Type of the tuples forming the input sequence. Each tuple in
                the input sequence must be of this type.
        """
        raise NotImplementedError()

    def iter_args(self, t: namedtuple) -> Iterable:
        """Iterate over arguments of output tuple

        Has to be implemented by the derived class

        Args:
            t: input tuple

        Yields:
            Arguments of the output tuple
        """
        raise NotImplementedError()

    @classmethod
    def apply(cls,
              records: Iterable[namedtuple],
              rectype: Optional[Type[namedtuple]] = None
              ) -> Tuple[Iterable[namedtuple], Type[namedtuple]]:
        """Apply name adapter to sequence if required

        Args:
            records: Iterable of input tuples
            rectype: Type of input records

        Returns:
            tuple containing a generator of output tuples and the type of the
            output tuples.
        """
        if rectype is None:
            seq = Sequence(records)
            rectype = seq.element_type
            records = seq
        f = cls(rectype) if rectype is not None else None
        if f:
            return map(f, records), f.record_type
        return records, rectype


class NameAdapter(AdapterBase):
    """Replaces fields containing the name by firstname and lastname fields

    Args:
        rectype: Type of the input tuple.
    """
    def set_fields(self, rectype: Type[namedtuple]) -> None:
        add = []
        for key in rectype._fields:
            if not key.endswith("name") or key.endswith("nickname"):
                copy, prefix = True, None
            elif key.endswith("first_name"):
                prefix = key[:-10]
                copy = f"{prefix}last_name" in rectype._fields
            elif key.endswith("last_name"):
                prefix = key[:-9]
                copy = f"{prefix}first_name" in rectype._fields
            else:
                copy, prefix = False, key[:-4]

            if copy:
                self._copy.append(key)
            else:
                names = [f"{prefix}{s}" for s in ("first_name", "last_name")]
                if not all(s in rectype._fields or s in add for s in names):
                    add.extend(names)
        if add:
            self._rectype = namedtuple(f"Modified{rectype.__name__}",
                                       self._copy + add)
            self._add = [f"{s[:-10]}name" for s in add[::2]]  # first names only

    def iter_args(self, t: namedtuple) -> Iterable:
        """Iterate over arguments of output tuple

        Args:
            t: input tuple

        Yields:
            Arguments of the output tuple
        """
        for f in self._copy:
            yield getattr(t, f)

        for f in self._add:
            n = getattr(t, f).split(",", maxsplit=1) + [""]
            last_name, first_name = n[:2]
            yield first_name.strip()
            yield last_name.strip()


class TimeAdapter(AdapterBase):
    """Joins separate date and time fields into a single field

    Args:
        rectype: Type of the input tuple.
    """
    def set_fields(self, rectype: Type[namedtuple]) -> None:
        for key in rectype._fields:
            if key.endswith("date"):
                prefix = key[:-4]
                copy = f"{prefix}time" not in rectype._fields
            elif key.endswith("time"):
                prefix = key[:-4]
                copy = f"{prefix}date" not in rectype._fields
            else:
                copy, prefix = True, None

            if copy:
                self._copy.append(key)
            else:
                if not (prefix in rectype._fields or prefix in self._add):
                    self._add.append(prefix)
        if self._add:
            add = [s.strip("_").strip() for s in self._add]
            self._rectype = namedtuple(f"Modified{rectype.__name__}",
                                       self._copy + add)

    def iter_args(self, t: namedtuple) -> Iterable:
        """Iterate over arguments of output tuple

        Args:
            t: input tuple

        Yields:
            Arguments of the output tuple
        """
        for f in self._copy:
            yield getattr(t, f)

        for f in self._add:
            _date = getattr(t, f"{f}date").strip()
            _time = getattr(t, f"{f}time").strip()
            yield f"{_date}T{_time}"
