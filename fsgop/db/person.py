import re
from datetime import date
from typing import Optional, Tuple, Union

from .record import Record, to
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
        count (int): An integer making the first_name last_name combination
            unique, if required.
        kind (int or str): The kind of person this record describes. Defaults to
            natural person, but organizations are possible, too.
        comments (str): Comments field.
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

    def index_tuple(self) -> Optional[tuple]:
        """Create index tuple

        Overrides the default implementation to force ``None`` is returned, if
        neither first nor last name are available.
        """
        if f"{self.first_name}{self.last_name}" == "":
            return None
        return super().index_tuple()

