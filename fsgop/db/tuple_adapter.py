from typing import Optional, Type, Iterable, Tuple
from collections import namedtuple

from .utils import Sequence


class AdapterBase(object):
    """Modifies each tuple in a sequence of tuples

    Args:
        rectype: Type of the tuples forming the input sequence. Each tuple in
            the input sequence must be of this type.
    """
    def __init__(self) -> None:
        self._copy = []  # list of fields copied from input tuples
        self._add = []   # list of additional fields created by this class
        self._result_type = None

    def __call__(self, t: namedtuple) -> namedtuple:
        """Convert input tuple to output tuple

        Args:
            t: Input tuple containing one or more combined name fields

        Returns:
            named tuple with combined fields replaced by first name and last name
            fields.
        """
        return self._result_type._make(self.iter_args(t))

    def __bool__(self) -> bool:
        """Check if any fields are added by this functor

        Returns:
            ``True`` if and only if this functor does anything useful

        """
        return bool(self._add)

    @property
    def result_type(self) -> Type[namedtuple]:
        """Get record type returned by this functor"""
        return self._result_type

    def configure_for(self, rectype: Type[namedtuple]) -> None:
        """Set fields to copy or add for a given record type

        Has to be implemented by the derived class.

        Args:
            rectype: Type of the tuples forming the input sequence. Each tuple in
                the input sequence must be of this type.
        """
        self._result_type = None

    def iter_args(self, t: namedtuple) -> Iterable:
        """Iterate over arguments of output tuple

        Has to be implemented by the derived class

        Args:
            t: input tuple

        Yields:
            Arguments of the output tuple
        """
        raise NotImplementedError()

    def apply_to(self,
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
        self._copy.clear()
        self._add.clear()
        self._result_type = None
        if rectype is None:
            seq = Sequence(records)
            rectype = seq.element_type
            records = seq

        if rectype is not None:
            # seq is not empty
            self.configure_for(rectype)
            if self:
                return map(self, records), self._result_type

        return records, rectype


class NameAdapter(AdapterBase):
    """Replaces fields containing the name by firstname and lastname fields

    Args:
        rectype: Type of the input tuple.
    """
    def configure_for(self, rectype: Type[namedtuple]) -> None:
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
            self._result_type = namedtuple(f"Modified{rectype.__name__}",
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


class DateTimeAdapter(AdapterBase):
    """Joins separate date and time fields into a single datetime field

    Args:
        delimiter: Delimiter used to join time and date strings
    """
    def __init__(self, delimiter: str = " "):
        super().__init__()
        self._delimiter = delimiter

    def configure_for(self, rectype: Type[namedtuple]) -> None:
        self._copy = list(rectype._fields)

        for prefix in ("begin", "end"):
            if f"{prefix}_date" in self._copy and f"{prefix}_time" in self._copy:
                self._copy.remove(f"{prefix}_date")
                self._copy.remove(f"{prefix}_time")
                if prefix not in self._copy:
                    self._add.append((f"{prefix}_date", f"{prefix}_time"))

        if "end_time" in self._copy and "end_date" not in self._copy:
            if "begin_date" in rectype._fields:
                self._copy.remove("end_time")
                self._add.append(("begin_date", "end_time"))

        if self._add:
            add = ["_".join(s2.split("_")[:-1]).strip() for s1, s2 in self._add]
            self._result_type = namedtuple(f"Modified{rectype.__name__}",
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

        for f1, f2 in self._add:
            yield self._delimiter.join((getattr(t, f1), getattr(t, f2)))


def apply(adapters: Iterable[AdapterBase],
          records: Iterable[namedtuple],
          rectype: Optional[Type[namedtuple]] = None
          ) -> Tuple[Iterable[namedtuple], Type[namedtuple]]:
    """Consecutively apply a number of adapters to a sequence

    Args:
        adapters: Iterable of adapters to apply to a sequence
        records: Input sequence
        rectype: Type of records in input sequence. Auto-detected from first
            record, if not specified.

    Returns:
        Adapted sequence and type of elements in this sequence
    """
    for adapter in adapters:
        records, rectype = adapter.apply_to(records, rectype)
    return records, rectype