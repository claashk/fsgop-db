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

    def to(self, t):
        """Convert to namedtuple

        Arguments:
            t (namedtuple): A namedtuple type

        Return:
            t: Namedtuple instance
        """
        return t._make(self.format(x) for x in t._fields)

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
    def attributes(cls):
        """Get attributes of this class

        Returns:
            set: Attributes accepted by this record
        """
        return set(vars(cls()).keys())

    @classmethod
    def from_dict(cls, d, attrs=None):
        """Create record from dictionary

        Arguments:
            d (dict): Dictionary containing attributes. Keys must match
                name of arguments passed to constructor
            attrs (iterable or None): Attributes to select from d. If ``None``,
                 cls.index is used.

        Returns:
            Record: Record instance
        """
        _attrs = set(cls.index) if attrs is None else attrs
        return cls(**{k: v for k, v in d.items() if k in _attrs})

    @classmethod
    def from_namedtuple(cls, t, attrs=None):
        """Create record from named tuple

        Arguments:
            t (namedtuple): Tuple containing attributes. Attribute names must
                match name of arguments passed to constructor
            attrs (iterable or None): Attributes to select from t. If ``None``,
                 cls.index is used.

        Returns:
            Record: Record instance
        """
        _attrs = set(cls.index) if attrs is None else attrs
        return cls(**{k: getattr(t, k) for k in t._fields if k in _attrs})