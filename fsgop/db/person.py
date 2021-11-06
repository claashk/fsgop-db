from .record import Record, to
from .utils import ASCII
import re
from datetime import date

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


def split_title(name):
    """Split title from (last) name

    Arguments:
        name (str): Name string including title

    Return:
        tuple: Name and title
    """
    groups = TITLE_PATTERN.split(name)
    title = ". ".join(groups[1::2])
    return groups[-1], f"{title}." if title else None


def split_count(name):
    """Split counter from name

    Arguments:
        name (str): Name followed by integer counter in parentheses

    Return:
         tuple: Name and count
    """
    m = COUNTER_PATTERN.match(name)
    if m is not None:
        return m.group(1).strip(), int(m.group(2))
    return name.strip(), None


class Person(Record):
    """Internal representation of a person

    Arguments:
        uid (int): Unique ID of this person in a table.
        last_name (str): Last name including any titles
        first_name (str): First name, possibly including a number in
            parentheses to make the first name last name combination unique.
        title (str): Title(s) of this person
        count (int): An integer making the first_name last_name combination
            unique, if required.
        kind (int or str): The kind of person this record describes. Defaults to
            natural person, but organizations are possible, too.
        comments (str): Comments field.
    """
    index = ["last_name", "first_name", "count"]

    def __init__(self,
                 uid=None,
                 last_name=None,
                 first_name=None,
                 title=None,
                 birthday=None,
                 birthplace=None,
                 count=None,
                 kind=None,
                 comments=None):
        super().__init__()
        self.uid = to(int, uid, default=None)
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
    def username(self):
        """Generate default user name of the form '``first_name``.``last_name``'
        
        Return:
            str: Username (all lowercase without special characters)
        """
        s1 = f"{self.first_name.split()[0].lower()}" if self.first_name else ""
        s2 = f"{self.last_name.split()[-1].lower()}" if self.last_name else ""
        s = f"{s1}.{s2}" if s1 and s2 else f"{s1 or s2}"
        if self.count > 1:
            s = f"{s}_{self.count}"
        return s.translate(ASCII)

