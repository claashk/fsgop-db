from typing import Optional, Union, Type, Iterable, Tuple
from datetime import date, datetime
import re
from collections import namedtuple
from .utils import Sequence

from .record import Record, to
from .property import Property
from .utils import ASCII

COUNTER_PATTERN = re.compile(r"(.+)\((\d+)\)")
TITLE_PATTERN = re.compile(r"(Prof|Dr|rer|nat|phil|jur|med|Ing|M.Sc)\.-?\s*")

PERSON_UNDEFINED = 0
PERSON_MALE = 1
PERSON_FEMALE = 2
PERSON_DIVERSE = 3
CLUB = 10
COMPANY = 20

PERSON_KINDS = {
    "male": PERSON_MALE,
    PERSON_MALE: PERSON_MALE,
    "female": PERSON_FEMALE,
    PERSON_FEMALE: PERSON_FEMALE,
    "diverse": PERSON_DIVERSE,
    PERSON_DIVERSE: PERSON_DIVERSE,
    "club": CLUB,
    CLUB: CLUB
}


def split_title(name: str) -> Tuple[str, Optional[str]]:
    """Split title from (last) name

    Args:
        name: Name string including title

    Returns:
        Name and title, where title can be ``None``.
    """
    groups = TITLE_PATTERN.split(name)
    title = ". ".join(groups[1::2])
    return groups[-1], f"{title}." if title else None


def split_count(name: str) -> Tuple[str, Optional[int]]:
    """Split counter from name

    Args:
        name: Name followed by integer counter in parentheses

    Returns:
         Name and count, where count is ``None`` if no counter could be found
    """
    m = COUNTER_PATTERN.match(name)
    if m is not None:
        return m.group(1).strip(), int(m.group(2))
    return name.strip(), None


class Person(Record):
    """Internal representation of a person

    Args:
        uid: Unique ID of this person in a table.
        last_name: Last name including any titles
        first_name: First name, possibly including a number in parentheses to
            make the first name last name combination unique.
        title: Title(s) of this person
        birthday: Birthday
        birthplace: Birthplace
        count: An integer making the first_name last_name combination unique, if
            required.
        kind: The kind of person this record describes. Defaults to natural
            person, but organizations are possible, too.
        comments: Comments field.
    """
    index = ["last_name", "first_name", "count"]

    def __init__(self,
                 uid: Optional[int] = None,
                 last_name: Optional[str] = None,
                 first_name: Optional[str] = None,
                 title: Optional[str] = None,
                 birthday: Optional[Union[str, date]] = None,
                 birthplace: Optional[str] = None,
                 count: Optional[int] = None,
                 kind: Optional[Union[str, int]] = None,
                 comments: Optional[str] = None) -> None:
        super().__init__(uid=uid)
        self.last_name = to(str, last_name, default="").strip()
        self.first_name = to(str, first_name, default="").strip()
        self.birthday = to(date, birthday, default=None)
        self.birthplace = to(str, birthplace, default=None)
        self.kind = None if kind is None else PERSON_KINDS[kind]

        self.comments = to(str, comments, default="").strip()

        if title is None:
            self.last_name, title = split_title(self.last_name)

        if count is None:
            self.first_name, count = split_count(self.first_name)

        self.title = to(str, title, default="").strip()
        self.count = to(int, count, default=1)

    @property
    def username(self) -> str:
        """Generate default user name of the form '``first_name``.``last_name``'
        
        Returns:
            Username (all lowercase without special characters)
        """
        s1 = f"{self.first_name.split()[0].lower()}" if self.first_name else ""
        s2 = f"{self.last_name.split()[-1].lower()}" if self.last_name else ""
        s = f"{s1}.{s2}" if s1 and s2 else f"{s1 or s2}"
        if self.count > 1:
            s = f"{s}_{self.count}"
        return s.translate(ASCII)

    @property
    def name(self):
        if self.first_name and self.last_name:
            s = f"{self.last_name}, {self.first_name}"
        else:
            s = self.last_name or self.first_name
        if self.count > 1:
            s = f"{s} ({self.count})"
        return s

    def index_tuple(self) -> Optional[tuple]:
        """Create index tuple

        Overrides the default implementation to force ``None`` is returned, if
        neither first nor last name are available.
        """
        if not (self.first_name or self.last_name):
            return None
        return super().index_tuple()


class PersonProperty(Property):
    """Person property implementation

    Arguments:
        uid: Unique ID of this property record
        person: Person this property describes. An integer is interpreted as
            uid.
        valid_from: Date from which on this property is valid. ``None`` if it is
           valid since the dawn of time
        valid_until: Date after which this property expires. Use ``None`` to
           indicate that the property does not expire
        kind: Kind of this property
        value: Property value
    """
    index = [x if x != "rec" else "person" for x in Property.index]

    def __init__(self,
                 uid: Optional[int] = None,
                 person: Optional[Union[Person, int]] = None,
                 valid_from: Optional[datetime] = None,
                 valid_until: Optional[datetime] = None,
                 kind: Optional[str] = None,
                 value: Optional[str] = None) -> None:
        super().__init__(uid=uid,
                         rec=to(Person, person, default=None),
                         valid_from=valid_from,
                         valid_until=valid_until,
                         kind=kind,
                         value=value)

    @property
    def person(self):
        return self.rec

    @classmethod
    def layout(cls,
               prefix: str = "",
               allow: Optional[Iterable[str]] = None) -> dict:
        """Get layout of this class

        Overrides the default implementation, which does not work for nested
        data models.

        Args:
             prefix: Prefix to add to all keys. Defaults to None
             allow: Iterable of allowed values. If not ``None``, only names in
                 this dictionary are included in the output. If a prefix is
                 provided, then values must include the prefix.

        Returns:
            Layout dictionary.
        """
        return cls._layout_helper(Person, prefix=prefix, allow=allow)


class NameAdapter(object):
    """Replaces fields containing the name by firstname and lastname fields

    Args:
        rectype: Type of the input tuple.
    """
    def __init__(self, rectype: Type[namedtuple]) -> None:
        self._copy = []
        self._add = []
        self._rectype = rectype
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
