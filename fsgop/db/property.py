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
           valid since the dawn of time. The property is valid at this time.
        valid_until: Date at which this property expires. Use ``None`` to
           indicate that the property does not expire. The property is not valid
           at this point in time.
        name: Name of this property
        value: Property value
    """
    index = ["rec", "name", "valid_until", "value"]

    def __init__(self,
                 uid: Optional[int] = None,
                 rec: Optional[Union[Record, int]] = None,
                 valid_from: Optional[datetime] = None,
                 valid_until: Optional[datetime] = None,
                 name: Optional[str] = None,
                 value: Optional[str] = None) -> None:
        super().__init__(uid=uid)
        self.rec = to(Record, rec, default=None)
        self.valid_from = to(datetime,
                             valid_from,
                             default=datetime(MINYEAR, 1, 1))
        self.valid_until = to(datetime,
                              valid_until,
                              default=datetime(MAXYEAR, 12, 31, 23, 59))
        self.name = to(str, name, default=None)
        self.value = to(str, value, default=None)

    def add_to(self, rec: Record):
        """Add this property to a record

        Args:
            rec: Record to which to add property
        """
        self.rec = rec
        rec[self.name].add(self)

    @staticmethod
    def get_from(rec: Record,
                 name: str,
                 at: datetime) -> Optional["Property"]:
        """Get a single property by name and date

        Args:
            rec: Record to get property from
            name: Name of the property to find
            at: Date at which this property is valid

        Returns:
            Matching Property or None if no matching property can be found

        Raises:
            ValueError if more than one matching property is found
        """
        m = tuple(p for p in rec[name] if p.valid_from <= at < p.valid_until)
        if not m:
            return None
        if len(m) > 1:
            raise ValueError(f"Expected at most one {name} property, "
                             f"found {len(m)}")
        return m[0]

    @staticmethod
    def discard_from(rec: Record, name: str, at: Optional[datetime] = None):
        """Remove property from a record

        Args:
            rec: Record from which to remove a property
            name: Name of property to discard.
            at: Date, at which property is valid. If not specified, all
                properties with matching key are discarded.
        """
        props = rec[name]
        if at is None:
            props.clear()
        else:
            erase = set(p for p in props if p.valid_from <= at < p.valid_until)
            props -= erase
