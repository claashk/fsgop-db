from typing import Union, Optional, Type, Iterable
from collections import namedtuple
from .utils import to


class Record(object):
    """Base class for records in a table

    Args:
        uid: Unique integer id of this record. Defaults to ``None``
    """
    index = []

    def __init__(self, uid: Optional[int] = None):
        self.uid = to(int, uid, default=None)

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
    def fields(cls) -> set:
        """Get all attributes/fields of this record type

        Returns:
            Names of all fields in this record
        """
        return set(vars(cls()).keys())

    @classmethod
    def nested_records(cls) -> set:
        """Get all attributes/fields of this record, which are also records

        Returns:
            Names of all fields in this record, which are also records
        """
        return {k for k, v in vars(cls()).items() if isinstance(v, Record)}

    @classmethod
    def fields_in(cls, names: Iterable[str], index_only: bool = False) -> set:
        """Get names of fields matching this record

        The output of this method can be used as input to :meth:`from_dict` and
        :meth:`from_obj`.

        Args:
            names: List of field names
            index_only (bool): If True, only matching fields from the index
               are returned

        Return:
            Names of recognised fields in *names*
        """
        if index_only:
            return (set(cls.index) - cls.nested_records()) & set(names)
        return (cls.fields() - cls.nested_records()) & set(names)

    @classmethod
    def from_dict(cls,
                  d: dict,
                  attrs: Optional[Iterable[str]] = None,
                  prefix: str = "") -> "Record":
        """Create record from dictionary

        Args:
            d: Dictionary containing attributes. Keys must match name of
              arguments passed to constructor
            attrs: Attributes to select from d. If ``None``, cls.index is used.
            prefix: Prefix appended to each attribute. Defaults to ``""``.

        Returns:
            Record instance

        Raises:
            KeyError: If one of the attributes in attrs is not found in d
        """
        _attrs = set(cls.index) if attrs is None else set(attrs)
        return cls(**{key: d[f"{prefix}{key}"] for key in _attrs})

    @classmethod
    def from_obj(cls,
                 obj: object,
                 attrs: Optional[Iterable[str]] = None,
                 prefix: str = "") -> "Record":
        """Create record from named tuple

        Arguments:
            obj: Object instance. Attributes must be identical to
                the attributes passed in attrs.
            attrs (iterable or None): Attributes to select from t. If ``None``,
                 cls.index is used.
            prefix (str): Prefix appended to each attribute. Defaults to ``""``.

        Returns:
            Record instance
        """
        _attrs = set(cls.index) if attrs is None else set(attrs)
        return cls(**{key: getattr(obj, f"{prefix}{key}") for key in _attrs})
