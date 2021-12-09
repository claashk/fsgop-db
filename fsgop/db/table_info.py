from typing import Optional, Iterable, Generator, Tuple, Callable
from collections import namedtuple


class ColumnInfo(object):
    """Stores meta data for a column of a (MySQL) Table

    Args:
        name: Column name
        dtype: Data type
        allows_null: ``True`` if and only if NULL is a valid value
        default_value: The default value for the column
        extra: Extra information, such as 'auto_increment'
        references: Name of a table, which is used to store information
            referenced in this column, e.g. "Person(uid)"
    """
    def __init__(self,
                 name: Optional[str] = None,
                 dtype: Optional[str] = None,
                 allows_null: bool = False,
                 default_value: Optional[str] = None,
                 extra: str = "",
                 references: Optional[str] = None) -> None:
        self.name = name
        self.dtype = dtype
        self.allows_null = allows_null
        self.default_value = default_value
        self.extra = extra
        self.references = references

    def has_auto_increment(self) -> bool:
        """Check if column is incremented automatically

        Returns:
            ``True`` if and only if the current column is incremented
            automatically
        """
        return "auto_increment" in self.extra

    def default(self) -> str:
        """Default value as escaped string required in MySQL statements

        Returns:
            '<val>' or NULL
        """
        if self.default_value is None:
            return "NULL"
        return f"'{self.default_value}'"

    def sql_references(self) -> str:
        """Get SQL reference statement for this column

        Returns:
            References statement or empty string, if no references are defined.
        """
        if self.references:
            return f" REFERENCES {self.references}"
        return ""


class IndexInfo(object):
    """Stores information about an index for a given table

    Args:
        name: Name of the index
        is_unique: ``True`` if and only if the index is unique
        is_primary: ``True`` if and only if this is the primary index.
        columns: List of tuples, where each tuple contains the arguments to
            :meth:`IndexInfo.add_column` for the column to add to this index.
    """
    def __init__(self,
                 name: str,
                 is_unique: bool = False,
                 is_primary: bool = False,
                 columns: Optional[Iterable[tuple]] = None) -> None:
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

    def __lt__(self, other: "IndexInfo") -> bool:
        """Less comparison by is_primary and name"""
        return (not self.is_primary, self.name) < (not other.is_primary,
                                                   other.name)

    def __gt__(self, other: "IndexInfo") -> bool:
        """Greater comparison by is_primary and name"""
        return (not self.is_primary, self.name) > (not other.is_primary,
                                                   other.name)

    def __ge__(self, other: "IndexInfo") -> bool:
        return not (self < other)

    def add_column(self,
                   name: str,
                   order: int = 1,
                   sequence: Optional[int] = None) -> None:
        """Add column to this index

        Args:
            name: Name of the column
            order: Order of the column ("1" for ascending, "-1" for descending,
                "0" for unsorted)
            sequence: Zero based index of the column within the index. Defaults
                to the current number of columns.
        """
        i = int(sequence) if sequence is not None else len(self._cols)
        n_missing = i + 1 - len(self._cols)
        if n_missing > 0:
            self._cols.extend(n_missing * [None])
        self._cols[i] = (str(name), int(order))

    def key_format(self) -> str:
        """Get SQL KEY as string"""
        return ",".join(f"{n}{' DESC' if i < 0 else ''}" for n, i in self._cols)

    def as_dict(self) -> dict:
        """Convert content to a dictionary"""
        return {
            "name": self.name,
            "is_unique": self.is_unique,
            "is_primary": self.is_primary,
            "columns": [(name, o) for name, o in self._cols]
        }


class TableInfo(object):
    """Stores meta data for a (MySql) table

    Args:
        name: Name of this table
        columns: Iterable containing one :class:`fsgop.db.ColumnInfo` object per
            column in this table.
        indices: Iterable containing one :class:`IndexInfo` object per index for
            this table.
    """
    def __init__(self,
                 name: Optional[str] = None,
                 columns: Optional[Iterable[ColumnInfo]] = None,
                 indices: Optional[Iterable[IndexInfo]] = None) -> None:
        self.name = name
        self._cols = []
        self._indices = dict()
        
        if columns is not None:
            for col in columns:
                self.add_column(col)

        if indices is not None:
            for idx in indices:
                self.add_index(idx)

    def __iter__(self) -> Generator[ColumnInfo, None, None]:
        """Iterate over the columns of this table

        Yields:
            Column information
        """
        yield from self._cols

    @property
    def ncols(self) -> int:
        """Get number of columns in this table

        Return:
            Number of columns in this table
        """
        return len(self._cols)

    @property
    def nidx(self) -> int:
        """Get number of indices for this table"""
        return len(self._indices)

    @property
    def columns(self) -> Tuple[Optional[str]]:
        """Get names of all columns in this table

        Returns:
            Name of each column in this table
        """
        return tuple(c.name for c in self._cols)

    @property
    def index_names(self) -> set:
        return set(self._indices.keys())

    def indices(self) -> Generator[IndexInfo, None, None]:
        """Iterate over all indices of this table

        Yields:
            tuple(str, :class:`IndexInfo`): index name and :class:`IndexInfo`
               object for each index
        """
        yield from self._indices.items()

    def add_column(self, col: ColumnInfo) -> None:
        """Insert a column into the table
        
        Args:
            col: Column information to insert
        """
        self._cols.append(col)

    def add_index(self, idx: IndexInfo) -> None:
        """Add an index for this table

        Args:
            idx: Index information to insert
        """
        self._indices[idx.name] = idx

    def format(self, placeholder: str = "%s") -> str:
        """Returns a tuple to be passed to INSERT INTO VALUES command.

        Args:
            placeholder: Placeholder used for values. Defaults to ``'%s'``.

        Returns:
            format string
        """
        return f"({','.join(self.ncols * [placeholder])})"

    def primary_key(self) -> str:
        """Get primary key statement

        Return:
            Format string for the first key marked as primary, empty string
            if no primary key is defined
        """
        for name, idx in self.indices():
            if idx.is_primary:
                return f"PRIMARY KEY ({idx.key_format()})"
        return ""

    def get_column(self, name: str) -> ColumnInfo:
        """Get column with a given name
        
        Args:
            name: Column name
        
        Returns:
            Column information

        Raise:
            KeyError: If no column with the given `name` exists.
        """
        for col in self._cols:
            if col.name == name:
                return col
        raise KeyError(f"No such column: '{name}'")

    def record_type(self,
                    name: Optional[str] = None,
                    aliases: Optional[dict] = None) -> namedtuple:
        """Create a named tuple for this table

        Args:
            name: Name of the returned namedtuple type
            aliases: Dictionary containing an alias as value for each column name
               (key) in this table. Column names not in aliases are not modified.
        """
        _alias = aliases if aliases is not None else dict()
        _name = name if name is not None else f"{self.name.capitalize()}Record"
        return namedtuple(_name, [_alias.get(k, k) for k in self.columns])

    def record_parser(self, get_parser: Callable) -> Tuple[Callable]:
        """Create a parser for this table

        Args:
            get_parser: Function returning a parser function for a column given
               the dtype string of the column as input
               (e.g. :meth:`fsgop.db.Database.get_parser`)

        Returns:
            One callable per column in this table, which converts strings to the
            appropriate data types of the column.
        """
        return tuple(get_parser(col.dtype) for col in self._cols)

    def as_dict(self) -> dict:
        """Convert table information into a dictionary

        Returns:
            Dictionary containing columns and indices of this table.
            The output can be imported using ``TableInfo.from_list(**output)``
        """
        return {"columns": [dict(vars(col).items()) for col in self._cols],
                "indices": [i.as_dict() for i in sorted(self._indices.values())]}

    @classmethod
    def from_list(cls,
                  name: str,
                  columns: Iterable[dict],
                  indices: Optional[Iterable[dict]] = None) -> "TableInfo":
        """Create a new TableInfo instance from a list of columns and indices

        Args:
            name: Table name
            columns: Iterable of dictionaries, where each dictionary can be
                forwarded as keyword arguments to the ColumInfo constructor.
            indices: Iterable of dictionaries, where each dictionary can be
                forwarded as keyword arguments to the IndexInfo constructor.

        Returns:
            Table information
        """
        table = cls(name=name)
        for col in columns:
            table.add_column(ColumnInfo(**col))

        if indices is not None:
            for idx in indices:
                table.add_index(IndexInfo(**idx))
        return table
