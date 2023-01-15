from typing import Optional, Union, Iterable, Type
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
        kind: String describing the kind of this property
        value: Property value
    """
    index = ["rec", "kind", "valid_until", "value"]

    def __init__(self,
                 uid: Optional[int] = None,
                 rec: Optional[Union[Record, int]] = None,
                 valid_from: Optional[datetime] = None,
                 valid_until: Optional[datetime] = None,
                 kind: Optional[str] = None,
                 value: Optional[str] = None) -> None:
        super().__init__(uid=uid)
        self.rec = to(Record, rec, default=None)
        self.valid_from = to(datetime,
                             valid_from,
                             default=datetime(MINYEAR, 1, 1))
        self.valid_until = to(datetime,
                              valid_until,
                              default=datetime(MAXYEAR, 12, 31, 23, 59))
        self.kind = to(str, kind, default=None)
        self.value = to(str, value, default=None)

    def is_valid(self, when: datetime = datetime.utcnow()) -> bool:
        """Check if property is valid at a given date

        Args:
            when: Datetime at which to check for validity. Defaults to current UTC

        Return:
            ``True`` if and only if property is valid at the given date.
        """
        return self.valid_from <= when < self.valid_until

    def add_to(self, rec: Record):
        """Add this property to a record

        Args:
            rec: Record to which to add property
        """
        self.rec = rec
        rec[self.kind].add(self)

    @staticmethod
    def get_from(rec: Record,
                 kind: str,
                 at: Optional[datetime] = None) -> Optional["Property"]:
        """Get a single property by name and date

        Args:
            rec: Record to get property from
            kind: Kind / name of the property to find
            at: Date at which this property is valid

        Returns:
            Matching Property or None if no matching property can be found

        Raises:
            ValueError if more than one matching property is found
        """
        if at is not None:
            m = tuple(p for p in rec[kind] if p.is_valid(when=at))
        else:
            m = tuple(p for p in rec[kind])
        if not m:
            return None
        if len(m) > 1:
            raise ValueError(f"Expected at most one {kind} property, "
                             f"found {len(m)}")
        return m[0]

    @staticmethod
    def discard_from(rec: Record, kind: str, at: Optional[datetime] = None):
        """Remove property from a record

        Args:
            rec: Record from which to remove a property
            kind: Kind of property to discard.
            at: Date, at which property is valid. If not specified, all
                properties with matching key are discarded.
        """
        props = rec[kind]
        if at is None:
            props.clear()
        else:
            erase = set(p for p in props if p.valid_from <= at < p.valid_until)
            props -= erase

    @classmethod
    def _layout_helper(cls,
                       rec_cls: Type,
                       prefix: str = "",
                       allow: Optional[Iterable[str]] = None) -> dict:
        """Helper to create layout of this class

        Overrides the default implementation, which does not work for nested
        data models.

        Args:
            rec_cls: Class of Record to which this property is associated
            prefix: Prefix to add to all keys. Defaults to ``None``
            allow: Iterable of allowed values. If not ``None``, only names in
                this dictionary are included in the output. If a prefix is
                provided, then values must include the prefix.

        Returns:
            Layout dictionary.
        """
        retval = super(Property, cls).layout(prefix=prefix, allow=allow)
        rec_name = rec_cls.__name__.lower()
        kwargs = rec_cls.layout(prefix=f"{prefix}{rec_name}_", allow=allow)
        if kwargs:
            retval[rec_name] = kwargs
        return retval
