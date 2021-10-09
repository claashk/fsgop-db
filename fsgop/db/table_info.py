from collections import namedtuple


class ColumnInfo(object):
    """Stores meta data for a column of a (MySQL) Table

    Arguments:
        name (str): Column name
        dtype (str): Data type
        allows_null (bool): ``True`` if and only if NULL is a valid value
        default_value (str): The default value for the column
        extra (str): Extra information, such as 'auto_increment'
        references(str): Name of a table, which is used to store information
            referenced in this column, e.g. "Person.uid"
    """
    def __init__(self,
                 name=None,
                 dtype=None,
                 allows_null=False,
                 default_value=None,
                 extra="",
                 references=None):
        self.name = name
        self.dtype = dtype
        self.allows_null = allows_null
        self.default_value = default_value
        self.extra = extra
        self.references = references

    def has_auto_increment(self):
        """Check if column is incremented automatically

        Return:
            bool: ``True`` if and only if the current column is incremented
            automatically
        """
        return "auto_increment" in self.extra

    def default(self):
        """Default value as escaped string required in MySQL statements

        Return:
            str: '<val>' or NULL
        """
        if self.default_value is None:
            return "NULL"
        return f"'{self.default_value}'"


class IndexInfo(object):
    """Stores information about an index for a given table

    Arguments:
        name (str): Name of the index
        unique (bool): ``True`` if and only if the index is unique
        primary (bool): ``True`` if and only if this is the primary index for
           the table.
        columns (iterable): List of tuples, where each tuple contains the
           arguments to :meth:`IndexInfo.add_column` for the column to add to
           this index.
    """
    def __init__(self, name, is_unique=False, is_primary=False, columns=None):
        self.name = name
        self.is_unique = bool(is_unique)
        self.is_primary = bool(is_primary)
        self._cols = []

        if columns is not None:
            for col in columns:
                self.add_column(*col)
            if None in self._cols:
                raise ValueError(f"Index '{self.name}' incomplete: Missing "
                                 f"information about {self._cols.count(None)} / "
                                 f"{len(self._cols)} indexed columns")

    def __lt__(self, other):
        return (not self.is_primary, self.name) < (not other.is_primary,
                                                   other.name)

    def __gt__(self, other):
        return (not self.is_primary, self.name) > (not other.is_primary,
                                                   other.name)

    def __ge__(self, other):
        return not (self < other)

    def add_column(self, name, order=1, sequence=None):
        """Add column to this index

        Arguments:
            name (str): Name of the column
            sequence (int): Zero based index of the column within the index
            order (int): Order of the column ("1" for ascending, "-1" for
               descending, or "0" for unsorted)
        """
        i = int(sequence) if sequence is not None else len(self._cols)
        n_missing = i + 1 - len(self._cols)
        if n_missing > 0:
            self._cols.extend(n_missing * [None])
        self._cols[i] = (str(name), int(order))

    def key_format(self):
        """Get SQL KEY"""
        return ",".join(f"{n}{' DESC' if i < 0 else ''}" for n, i in self._cols)

    def as_dict(self):
        return {
            "name": self.name,
            "is_unique": self.is_unique,
            "is_primary": self.is_primary,
            "columns": [(name, o) for name, o in self._cols]
        }


class TableInfo(object):
    """Stores meta data for a (MySql) table

    Arguments:
        name (str): Name of this table
        columns (iterable): Iterable containing one :class:`fsgop.db.ColumnInfo`
            object per column in this table.
        indices (iterable): Iterable containing one :class:`IndexInfo` object
           per index for this table.
    """
    def __init__(self, name=None, columns=None, indices=None):
        self.name = name
        self._cols = []
        self._indices = dict()
        
        if columns is not None:
            for col in columns:
                self.add_column(col)

        if indices is not None:
            for idx in indices:
                self.add_index(idx)

    def __iter__(self):
        """Iterate over the columns of this table

        Yields:
            :class:`ColumnInfo`: column information
        """
        yield from self._cols

    @property
    def ncols(self):
        """Get number of columns in this table

        Return:
            int: Number of columns in this table
        """
        return len(self._cols)

    @property
    def nidx(self):
        return len(self._indices)

    @property
    def columns(self):
        """Get names of all columns in this table

        Return:
            tuple: Name of each column in this table
        """
        return tuple(c.name for c in self._cols)

    @property
    def index_names(self):
        return set(self._indices.keys())

    def indices(self):
        """Iterate over all indices of this table

        Yields:
            tuple(str, :class:`IndexInfo`): index name and :class:`IndexInfo`
               object for each index
        """
        yield from self._indices.items()

    def add_column(self, col):
        """Insert a column into the table
        
        Arguments:
            col (:class:`fsgop.db.ColumnInfo`): Column to insert
        """
        self._cols.append(col)

    def add_index(self, idx):
        """Add an index for this table

        Arguments:
            idx (:class:`fsgop.db.IndexInfo`): Index to insert
        """
        self._indices[idx.name] = idx

    def format(self):
        """Returns a tuple to be passed to INSERT INTO VALUES command.
        
        Return:
            str: format string
        """
        return f"({','.join(self.ncols * ['%s'])})"

    def primary_key(self):
        """Get primary key statement

        Return:
            str: Format string for the first key marked as primary, empty string
            if no primary key is defined
        """
        for name, idx in self.indices():
            if idx.is_primary:
                return f"PRIMARY KEY ({idx.key_format()})"
        return ""

    def get_column(self, name):
        """Get column with a given name
        
        Arguments:
            name(string): Column name to search for
        
        Return:
            pysk.SqlColumn instance with the given name. Raises a KeyError if no
            such column exists

        Raise:
            KeyError: If no column with the given `name` exists.
        """
        for col in self._cols:
            if col.name == name:
                return col
        raise KeyError(f"No such column: '{name}'")

    def iter_record(self, rec):
        """Iterates over the columns of a given object

        Arguments:
            rec (object): Object representing one record in this table. Shall
                contain an attribute for each column in this table, where the
                attribute name must match the respective name of the column in
                this table.

        Yields:
            object: Attributes of ``cls`` corresponding to the columns of this
                table.
        """
        for col in self._cols:
            yield getattr(rec, col.name)

    def get_record(self, rec):
        """Convert class into tuple based on attributes.
        
        Arguments:
            rec (object): Object representing one record in this table. Shall
                contain an attribute for each column in this table, where the
                attribute name must match the respective name of the column in
                this table.
        
        Return:
            tuple: Tuple intended for insertion into table
        """
        return tuple(self.iter_record(rec))

    def record_type(self, name):
        return namedtuple(name, self.columns)

    def as_dict(self):
        """Convert table information into a dictionary

        Returns:
            dict: Dictionary containing columns and indices of this table.
            The output can be imported using ``TableInfo.from_list(**output)``
        """
        return {"columns": [dict(vars(col).items()) for col in self._cols],
                "indices": [i.as_dict() for i in sorted(self._indices.values())]}

    @classmethod
    def from_list(cls, name, columns, indices=None):
        """Create a new TableInfo instance from a list of columns and indices

        Arguments:
            columns (list): Iterable of dictionaries, where each dictionary
               can be forwarded as keyword arguments to the ColumInfo
               constructor.
            indices (iterable): Iterable of dictionaries, where each dictionary
               can be forwarded as keyword arguments to the IndexInfo
               constructor.

        Return:
            TableInfo: Table information
        """
        table = cls(name=name)
        for col in columns:
            table.add_column(ColumnInfo(**col))

        if indices is not None:
            for idx in indices:
                table.add_index(IndexInfo(**idx))
        return table
