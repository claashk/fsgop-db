from typing import Optional, Union
from datetime import datetime, MINYEAR, MAXYEAR
from .record import Record, to


class Property(Record):
    """Generic property

    Arguments:
        uid: Unique ID of this property record
        rec: Record (person, airplane, ...) , this property describes. An integer
            is interpreted as record uid
        valid_from: Date from which on this property is valid. ``None`` if it is
           valid since the dawn of time
        valid_until: Date after which this property expires. Use ``None`` to
           indicate that the property does not expire
        key: Key describing this property
        value: Property value
    """
    index = ["rec", "key", "valid_until"]

    def __init__(self,
                 uid: Optional[int] = None,
                 rec: Optional[Union[Record, int]] = None,
                 valid_from: Optional[datetime] = None,
                 valid_until: Optional[datetime] = None,
                 key: Optional[str] = None,
                 value: Optional[str] = None) -> None:
        super().__init__(uid=uid)
        self.rec = to(Record, rec, default=None)
        self.valid_from = to(datetime,
                             valid_from,
                             default=datetime(MINYEAR, 1, 1))
        self.valid_until = to(datetime,
                              valid_until,
                              default=datetime(MAXYEAR, 12, 31, 23, 59))
        self.key = to(str, key, default=None)
        self.value = to(str, value, default=None)

