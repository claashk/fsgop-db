from .utils import to


class Record(object):
    """Base class for records in a table

    Arguments:

    """
    index = []

    def __init__(self):
        pass

    def __str__(self):
        """Convert instance to string
        
        Return:
            str: String representation of this record
        """
        return " ".join([self.format(x) for x in self.index])

    def __repr__(self):
        return f"{type(self).__name__} ({str(self)})"

    def __eq__(self, other):
        """Equal comparison
        
        Two pilots are defined equal, if and only if their first and last names
        are equal.
        
        Arguments:
            other (:class:`~fsgop.db.Record`): Record to compare to
        
        Return:
            bool: ``True`` if and only if ``self`` and *other* are equal.
        """
        try:
            return self.index_tuple() == other.index_tuple()
        except AttributeError:
            return False

    def __lt__(self, other):
        """Less comparison by last name and first name
                
        Arguments:
            other (:class:`~fsgop.db.Record`): Record to compare to
        
        Return:
            bool: ``True`` if and only if ``self`` is less than *other*.
        """
        return self.index_tuple() < other.index_tuple()
    
    def __hash__(self):
        """Hash for this record
        
        Return:
            str: hash of index tuple
        """
        return hash(self.index_tuple())

    def index_tuple(self):
        """Get index tuple

        Return:
            tuple: Tuple containing the indexed attributes of this record
        """
        return tuple(getattr(self, x) for x in self.index)

    def to(self, t, format=False):
        """Convert to namedtuple

        Arguments:
            t (namedtuple): A namedtuple type
            format (bool): If ``True`` field values will be converted to
                strings. Otherwise, they will be passed as native types.

        Return:
            t: Namedtuple instance
        """
        if format:
            return t._make(self.format(x) for x in t._fields)
        return t._make(getattr(self, x) for x in t._fields)

    def format(self, name):
        """Convert an attribute to a string

        This function is called by various exporters. This default implementation
        simply returns ``str(x)``. Override for special treatment of certain
        attributes.

        Arguments:
            name (str): Name of attribute

        Return:
            str: String representation of attribute ``name``.

        """
        return to(str, getattr(self, name), default="")

    @classmethod
    def fields(cls):
        """Get all attributes/fields of this record type

        Returns:
            set: Names of all fields in this record
        """
        return set(vars(cls()).keys())

    @classmethod
    def nested_records(cls):
        """Get all attributes/fields of this record, which are also records

        Returns:
            set: Names of all fields in this record, which are also records
        """
        return {k for k, v in vars(cls()).items() if isinstance(v, Record)}

    @classmethod
    def fields_in(cls, names, index_only=False):
        """Get names of fields matching this record

        The output of this method can be used as input to :meth:`from_dict` and
        :meth:`from_obj`.

        Arguments:
            names (iterable): List of field names
            index_only (bool): If True, only matching fields from the index
               are returned

        Return:
            set: List of recognised fields in names
        """
        if index_only:
            return (set(cls.index) - cls.nested_records()) & set(names)
        return (cls.fields() - cls.nested_records()) & set(names)

    @classmethod
    def from_dict(cls, d, attrs=None, prefix=""):
        """Create record from dictionary

        Arguments:
            d (dict): Dictionary containing attributes. Keys must match
                name of arguments passed to constructor
            attrs (iterable or None): Attributes to select from d. If ``None``,
                 cls.index is used.
            prefix (str): Prefix appended to each attribute. Defaults to ``""``.

        Returns:
            Record: Record instance

        Raises:
            KeyError: If one of the attributes in attrs is not found in d
        """
        _attrs = set(cls.index) if attrs is None else set(attrs)
        return cls(**{key: d[f"{prefix}{key}"] for key in _attrs})

    @classmethod
    def from_obj(cls, obj, attrs=None, prefix=""):
        """Create record from named tuple

        Arguments:
            obj (object): Object instance. Attributes must be identical to
                the attributes passed in attrs.
            attrs (iterable or None): Attributes to select from t. If ``None``,
                 cls.index is used.
            prefix (str): Prefix appended to each attribute. Defaults to ``""``.

        Returns:
            Record: Record instance
        """
        _attrs = set(cls.index) if attrs is None else set(attrs)
        return cls(**{key: getattr(obj, f"{prefix}{key}") for key in _attrs})