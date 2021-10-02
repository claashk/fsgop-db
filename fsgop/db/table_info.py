from collections import namedtuple


class ColumnInfo(object):
    """Stores meta data for a column of a (MySQL) Table

    Arguments:
        name (str): Column name
        dtype (str): Data type
        allows_null (bool): ``True`` if and only if NULL is a valid value
        index (str): Index type of this column. PRI (primary), UNI (unique) or
           MUL (multiple)
        default_value (str): The default value for the column
        extra (str): Extra information, such as 'auto_increment'
        references(str): Name of a table, which is used to store information
            referenced in this column, e.g. "Person.uid"
    """
    def __init__(self,
                 name=None,
                 dtype=None,
                 allows_null=False,
                 index=None,
                 default_value=None,
                 extra="",
                 references=None):
        self.name = name
        self.dtype = dtype
        self.allows_null = allows_null
        self.index = index
        self.default_value = default_value
        self.extra = extra
        self.references = references

    def is_primary_index(self):
        """Check if column is the primary index

        Return:
            bool: ``True`` if and only if the current column is the primary index
        """
        return self.index == "PRI"

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


class TableInfo(object):
    """Stores meta data for a (MySql) table

    Arguments:
        columns (iterable): Iterable containing one :class:`fsgop.db.ColumnInfo`
            object per column in this table.
    """
    def __init__(self, columns=None):
        """Construct new table    
    
        """    
        self._cols = []
        
        if columns is not None:
            for col in columns:
                self.add_column(col)

    @property
    def ncols(self):
        """Get number of columns in this table

        Return:
            int: Number of columns in this table
        """
        return len(self._cols)

    @property
    def columns(self):
        """Get names of all columns in this table

        Return:
            tuple: Name of each column in this table
        """
        return tuple(c.name for c in self._cols)

    def add_column(self, col):
        """Insert a column into the table
        
        Arguments:
            col (:class:`fsgop.db.ColumnInfo`): Column to insert
        """
        self._cols.append(col)

    def format(self):
        """Returns a tuple to be passed to INSERT INTO VALUES command.
        
        Return:
            str: format string
        """
        return f"({','.join(self.ncols * ['%s'])})"

    def get_column(self, name):
        """Get column with a given name
        
        Arguments:
            name(string): Column name to search for
        
        Return:
            pysk.SqlColumn instance with the given name. Raises a KeyError if no
            such column exists
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

    def export(self):
        return [{k: to(str, v) for k, v in vars(col).items()}
                for col in self._cols]

    @classmethod
    def from_list(cls, lst):
        table = cls()
        for col in lst:
            table.add_column(ColumnInfo(**col))

