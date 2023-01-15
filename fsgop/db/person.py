from typing import Optional, Union, Iterable, Tuple, List
from datetime import date, datetime
import re

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

    def valid_licences(self, when: datetime = datetime.utcnow()) -> List[Property]:
        """Get list of valid licences this person holds at a given time

        Args:
            when: Date and time at which to check for licences. Defaults to right
                now.

        Return:
            List containing one Property for each licence the current Person
            holds at the specified time.
        """
        return [p for p in self["licence"] if p.is_valid(when=when)]

    def holds_licence(self,
                      licences: Iterable[str],
                      when: datetime = datetime.utcnow()) -> bool:
        """Check if person holds any one of a number of required licences

        Args:
            licences: Iterable of strings containing the first letters of the
                required licence (e.g. `SPL` or FI-PPL(A)).
            when: Point in time at which to check for the licence.
        """
        for lic in self.valid_licences(when=when):
            for kind in licences:
                if lic.value.starts_with(kind):
                    return True
        return False

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
