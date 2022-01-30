from typing import Union, Optional, Type, Iterable
from collections import namedtuple
from inspect import signature
from .utils import to
from .database import Database


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
        return " ".join([self.format(x) for x in self.index])

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

    def to(self, t: Type[namedtuple], convert=False) -> namedtuple:
        """Convert to namedtuple

        Args:
            t: A namedtuple type
            convert (bool): If ``True`` field values will be converted to
                strings. Otherwise, they will be passed as native types.

        Returns:
            namedtuple derived object of type `t`.
        """
        if convert:
            return t._make(self.format(x) for x in t._fields)
        return t._make(getattr(self, x) for x in t._fields)

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

    def format(self, name: str) -> str:
        """Convert an attribute to a string

        This function is called by various exporters. This default implementation
        simply returns ``str(x)``. Override for special treatment of certain
        attributes.

        Args:
            name: Attribute name

        Returns:
            Attribute ``name`` converted to string
        """
        return to(str, getattr(self, name), default="")

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
             allow: Iterable of allowed values. If not ``None``, only names in
                 this dictionary are included in the output. If a prefix is
                 provided, then values must include the prefix.

        Returns:
            Layout dictionary.
        """
        retval = {k: f"{prefix}{k}" for k in signature(cls).parameters.keys()}
        if allow is not None:
            retval = {k: v for k, v in retval.items() if v in allow}
        return retval
