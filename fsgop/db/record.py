from typing import Union, Optional, Type, Iterable, Iterator
from collections import namedtuple
from inspect import signature
from datetime import datetime, date, time
from .utils import to
from .database import Database


def _to(obj, cls):
    if obj is None or (isinstance(obj, Record) and not obj):
        return None
    if isinstance(obj, cls):
        return obj
    if issubclass(cls, datetime):
        if isinstance(obj, date):
            return datetime.combine(obj, time(hour=0, minute=0))
    if issubclass(cls, date):
        if isinstance(obj, datetime):
            return obj.date()
    try:
        return cls(obj)
    except TypeError:
        pass
    raise TypeError(f"Unable to convert {obj} to {cls}")


class Record(object):
    """Base class for records in a table

    Args:
        uid: Unique integer id of this record. Defaults to ``None``
    """
    index = []

    def __init__(self, uid: Optional[int] = None):
        self.uid = to(int, uid, default=None)
        self._properties = dict()

    def __str__(self) -> str:
        """Convert this to string consisting of index fields
        """
        return ",".join([str(x) if x is not None else ""
                         for x in self.index_tuple()])

    def __int__(self) -> int:
        """Convert this record to an integer

        Raises:
            TypeError: If no uid has been assigned to this record
        """
        return int(self.uid)

    def __bool__(self):
        return self.key() is not None

    def __getitem__(self, name: str) -> set:
        """Get set of properties for a given name

        Args:
            name: Name of the property

        Returns:
            Set of properties with the given name
        """
        return self._properties.setdefault(name, set())

    def __repr__(self) -> str:
        return f"{type(self).__name__} ({str(self)})"

    def __eq__(self, other: "Record") -> bool:
        """Equal comparison by key
        
        Two records are equal, if and only if their respective keys are identical
        
        Args:
            other: Record to compare to
        
        Returns:
            ``True`` if and only if ``self`` and *other* are equal.
        """
        if isinstance(other, Record):
            return self.key() == other.key()
        return self.key() == other

    def __lt__(self, other: "Record") -> bool:
        """Less comparison by key
                
        Args:
            other: Record to compare to
        
        Returns:
            ``True`` if and only if ``self`` is less than *other*.
        """
        return self.key() < other.key()
    
    def __hash__(self) -> int:
        """Hash for this record
        
        Returns:
            hash of ``self.key()``
        """
        return hash(self.key())

    @property
    def properties(self) -> Iterator["Record"]:
        """Iterate over all property records of this record

        Yields:
            :class:`~fsgop.db.Property` records of this record
        """
        for v in self._properties.values():
            yield from v

    @property
    def has_properties(self) -> bool:
        """Check if this record has any properties

        Return:
            ``True`` if and only if this record has at least one property
        """
        return any(p for p in self._properties.values())

    def select(self, cls) -> Iterator[tuple]:
        """Select members by type

        Args:
            cls: Type of attributes to select

        Yields:
            tuple containing attribute name and attribute of all attributes
            with type 'cls'.
        """
        for k, v in vars(self).items():
            if isinstance(v, cls):
                yield k, v
            elif isinstance(v, Record) and v is not self:
                for kk, vv in v.select(cls):
                    yield f"{k}.{kk}", vv

    def index_tuple(self) -> Optional[tuple]:
        """Get index tuple

        Returns:
            Tuple containing the indexed attributes of this record or ``None``,
            if any index component is ``None``
        """
        t = tuple(getattr(self, x) for x in self.index)
        if None in t:
            return None
        return t

    def key(self) -> Optional[Union[int, tuple]]:
        """Get unique key for this record

        Returns:
            Index tuple if it is complete, if the index tuple is incomplete the
            uid will be returned.
        """
        idx = self.index_tuple()
        return idx if idx is not None else self.uid

    def to(self,
           t: Type[namedtuple],
           types: Optional[namedtuple] = None) -> namedtuple:
        """Convert to namedtuple

        Args:
            t: A namedtuple type
            types: Types of each record in t. Must be an instance of t. If not
               ``None``, each record in t is converted to the associated type
               in `types`. Defaults to ``None``.

        Returns:
            namedtuple derived object of type `t`.
        """
        rec = t._make(getattr(self, x) for x in t._fields)
        if types is None:
            return rec
        return t(*(_to(xi, ti) for xi, ti in zip(rec, types)))

    def search_in(self, db: Database, table: str) -> namedtuple:
        """Search this record in the database

        If this record has a uid, then the lookup will be by uid. If no uid
        is specified, the record will be completed by the index.

        Args:
            db: Database to search
            table: Name of table to search

        Returns:
            Matching database record.

        Raises:
            KeyError if no unique matching record is found
        """
        if self.uid is not None:
            return db.unique_id(table, self.uid)
        kwargs = {k: getattr(self, k) for k in self.index}
        _where = " and ".join(f"{k}={db.var(k)}" for k in kwargs.keys())
        return db.unique(table, where=_where, **kwargs)

    @classmethod
    def layout(cls, prefix: str = "", allow: Iterable[str] = None) -> dict:
        """Get layout of this class

        The layout is a map, which associates each keyword argument accepted
        by the constructor with the name of an attribute. The attribute value can
        then be passed as keyword argument to the constructor, e.g. using
        :func:`~fsgop.db.utils.kwargs_from`.

        This default implementation works for non-nested classes only.
        Args:
             prefix: Prefix to add to all keys. Defaults to None
             allow: Allowed attribute names. If not ``None``, only names in
                 this iterable are included in the output. If a prefix is
                 provided, then values must include the prefix.

        Returns:
            Layout dictionary.
        """
        retval = {k: f"{prefix}{k}" for k in signature(cls).parameters.keys()}
        if allow is not None:
            retval = {k: v for k, v in retval.items() if v in allow}
        return retval
