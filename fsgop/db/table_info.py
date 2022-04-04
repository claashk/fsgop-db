from typing import Optional, Iterable, Generator, Tuple, NamedTuple, Union, Dict
from typing import Type, List
import re
from collections import namedtuple
from datetime import datetime, date
from pathlib import Path
from copy import deepcopy

from .table_io import CsvParser


class ColumnInfo(object):
    """Stores metadata for a column of a (MySQL) Table

    Args:
        name: Column name
        dtype: Data type
        allows_null: ``True`` if and only if NULL is a valid value
        force_null: If ``True`` any value evaluated as ``False`` (i.e. 0, "") is
            converted to NULL: Defaults to ``False``.
        default_value: The default value for the column
        extra: Extra information, such as 'auto_increment'
        references: Name of a table, which is used to store information
            referenced in this column, e.g. "Person(uid)"
        fmt: Format string for this column. Used by parser in some cases.
    """
    references_pattern = re.compile(r"(\w+)\s*\((\w+)\)")
    default_date_format = "%Y-%m-%d"
    default_time_format = "%H:%M:%S"
    default_datetime_format = "%Y-%m-%d %H:%M:%S"

    def __init__(self,
                 name: Optional[str] = None,
                 dtype: Optional[str] = None,
                 allows_null: bool = False,
                 force_null: bool = False,
                 default_value: Optional[str] = None,
                 extra: str = "",
                 references: Optional[str] = None,
                 fmt: Optional[str] = None) -> None:
        self.name = name
        self.dtype = dtype
        self.allows_null = allows_null
        self.force_null = force_null
        self.default_value = default_value
        self.extra = extra
        self.references = references
        self.fmt = str(fmt) if fmt is not None else None
        self._parser = None
        self._native_type = None
        self._set_parser()

    @property
    def ref_info(self) -> tuple:
        """Split references into table name and column name

        Returns:
            Table name and column name referenced by this column, tuple of two
            ``None`` instances, if nothing is referenced.
        """
        if self.references:
            m = self.references_pattern.match(self.references)
            if m:
                return m.group(1), m.group(2)
            raise ValueError(f"Invalid reference string: '{self.references}'")
        return None, None

    @property
    def native_type(self) -> Type:
        return self._native_type

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

    @staticmethod
    def _force_none(f):
        return lambda x: f(x) or None

    @staticmethod
    def _int_parser(s: str) -> Optional[int]:
        """Default parser for integer values

        Args:
            s: Input string

        Returns:
            Integer
        """
        if s is None or s == r"\N":
            return None
        return int(s)

    @staticmethod
    def _float_parser(s: str) -> Optional[float]:
        """Default parser for float values

        Args:
            s: Input string

        Returns:
            float
        """
        if s is None or s == r"\N":
            return None
        return float(s)

    @staticmethod
    def _str_parser(s: str) -> Optional[str]:
        """Default parser for datetime strings

        Args:
            s: Input string

        Returns:
            string version of s
        """
        if s is None or s == r"\N":
            return None
        return str(s)

    def _date_parser(self, s: str) -> Optional[date]:
        """Default parser for date strings

        Args:
            s: Input string

        Returns:
            date instance
        """
        if s is None or s == r"\N":
            return None
        return datetime.strptime(s, self.fmt).date()

    def _datetime_parser(self, s: str) -> Optional[date]:
        """Default parser for date strings

        Args:
            s: Input string

        Returns:
            date instance
        """
        if s is None or s == r"\N":
            return None
        return datetime.strptime(s, self.fmt)

    def _set_parser(self):
        """Set parser for this column
        """
        if self.dtype is None:
            return
        s = self.dtype.lower()
        if "int" in s:
            self._native_type = int
            self._parser = self._int_parser
            # 'int' is not sufficient to create integer primary keys in sqlite:
            #  https://www.sqlite.org/lang_createtable.html#rowid
            self.dtype = "integer"
        elif "real" in s or "floa" in s or "doub" in s:
            self._native_type = float
            self._parser = self._float_parser
        elif s == "date":
            self._native_type = date
            self._parser = self._date_parser
            if self.fmt is None:
                self.fmt = self.default_date_format
        elif s == "datetime":
            self._native_type = datetime
            self._parser = self._datetime_parser
            if self.fmt is None:
                self.fmt = self.default_datetime_format
        else:
            self._native_type = str
            self._parser = self._str_parser

        if self.allows_null and self.force_null:
            self._parser = self._force_none(self._parser)


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

    @property
    def is_id(self) -> bool:
        """Return True if and only if this index is the unique ID index"""
        return self.is_primary and len(self._cols) == 1 and self._cols[0][1] == 1

    @property
    def columns(self) -> tuple:
        """Get tuple of all columns in this index"""
        return tuple(c[0] for c in self._cols)

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
    """Stores metadata for a (MySql) table

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
        self._record_type = None
        
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

    @property
    def record_type(self) -> Type[NamedTuple]:
        """Get native record type

        Returns:
            namedtuple: Named tuple instance with one field per column in this
            table.
        """
        if self._record_type is None:
            self.reset_record_type()
        return self._record_type

    @property
    def column_types(self) -> Type[namedtuple]:
        """Get type of each column

        Returns:
            named tuple containing the type for each column
        """
        return self.record_type(*(c.native_type for c in self._cols))

    def get_references(self) -> dict:
        """Get information about references in this table

        Returns:
            Dictionary with referenced table name as key and a set as value. The
            set contains the names of all columns in the referenced table which
            are referenced by this table.
        """
        retval = dict()
        for col in self._cols:
            table, column = col.ref_info
            if table:
                retval.setdefault(table, set()).add(column)
        return retval

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
        self._record_type = None

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

    @property
    def id_column(self) -> Optional[str]:
        """Get name of ID column

        Return:
            Name of ID column, None if no unique ID column exists
        """
        for name, idx in self.indices():
            if idx.is_id:
                return idx.columns[0]
        return None

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

    def create_record_type(self,
                           name: Optional[str] = None,
                           aliases: Optional[dict] = None) -> Type[namedtuple]:
        """Reset the internal record type to a named tuple for this table

        Args:
            name: Name of the returned namedtuple type. Defaults to
                ``'<name>Record'`` where <name> is the table name.
            aliases: Dictionary containing an alias as value for each column name
               (key) in this table. Column names not in aliases are not modified.

        Returns:
            named tuple type for records of this table
        """
        _alias = aliases if aliases is not None else dict()
        if name is not None:
            _n = name
        else:
            _n = f"{''.join(s.capitalize() for s in self.name.split('_'))}Record"
        return namedtuple(_n, [_alias.get(k, k) for k in self.columns])

    def reset_record_type(self,
                          name: Optional[str] = None,
                          aliases: Optional[dict] = None) -> None:
        """Reset the internal record type to a named tuple for this table

        Args:
            name: Name of the returned namedtuple type. Defaults to
                ``'<name>Record'`` where <name> is the table name.
            aliases: Dictionary containing an alias as value for each column name
               (key) in this table. Column names not in aliases are not modified.
        """
        self._record_type = self.create_record_type(name, aliases)

    def as_dict(self) -> dict:
        """Convert table information into a dictionary

        Returns:
            Dictionary containing columns and indices of this table.
            The output can be imported using ``TableInfo.from_list(**output)``
        """
        return {
            "columns": [
                {k: v for k, v in vars(col).items() if not k.startswith("_")}
                for col in self._cols
            ],
            "indices": [i.as_dict() for i in sorted(self._indices.values())]
        }

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

    def read_mysql_dump(self,
                        path: Union[str, Path],
                        reader: Optional[CsvParser] = None,
                        aliases: Optional[dict] = None) -> Generator[NamedTuple,
                                                                     None,
                                                                     None]:
        """Iterate over records of a MySql dump of this table

        Args:
            path: Path to input file
            reader: CSV file parser. If ``None``, a new default reader will be
                constructed.
            aliases: Dictionary containing a column name and an alias for
                selected columns. The resulting namedtuple will use the aliases
                as column names. Names not found in aliases remain unchanged.

        Yields:
            One namedtuple per record of the input file
        """
        if reader is None:
            reader = CsvParser()
        _rec = self.create_record_type(aliases=aliases)
        parsers = tuple(col._parser for col in self._cols)

        for rec in reader(str(path), skip_rows=0, delimiter="\t"):
            yield _rec(*(p(x) for p, x in zip(parsers, rec)))


def sort_tables(tables: Iterable[TableInfo]) -> List[TableInfo]:
    """Sorts table by name and references

    Sorts a sequence of tables, such that tables can be created in the given
    order without violation of reference constraints.

    Args:
        tables: Sequence of input tables

    Returns:
        list: Sorted list of tables
    """
    _tables = {table.name: table for table in tables}
    _unsorted = set(_tables.keys())
    _sorted = []

    while _unsorted:
        tmp = [k for k in _unsorted
               if all(t in _sorted for t in _tables[k].get_references().keys())]
        if not tmp:
            raise ValueError("Dependency loop detected")
        for name in sorted(tmp):
            _sorted.append(name)
            _unsorted.remove(name)

    return [_tables[k] for k in _sorted]


def dependencies(schema: dict, table: str) -> Generator[tuple, None, None]:
    """Iterate over tables & columns referencing a column in the specified table

    Finds all tables and columns in the given schema which reference a column of
    the specified table.

    Args:
        schema: Schema
        table: Name of table for which dependencies shall be checked

    Yields:
        Tuple containing:
        * name of referenced column in *table*
        * name of referencing table
        * name of referencing column in referencing table
    """
    if table not in schema.keys():
        raise ValueError(f"No such table in schema: '{table}'")
    for table_name, table_info in schema.items():
        for col in table_info:
            _table, _col = col.ref_info
            if _table == table:
                yield _col, table_name, col.name


def to_schema(d: Dict[str, Union[TableInfo, dict]]) -> Dict[str, TableInfo]:
    """Create a database schema from a dictionary

    A schema is a dictionary containing table names as keys and an associated
    TableInfo object as value.

    Args:
        d: Dictionary containing strings as keys and either a TableInfo object or
            a dictionary with key value arguments as value. Dictionaries will
            be converted to TableInfo objects using TableInfo.from_list.

    Returns:
        Valid schema constructed from input dictionary
    """
    _dict = deepcopy(d)
    return {k: v if isinstance(v, TableInfo) else TableInfo.from_list(k, **v)
            for k, v in _dict.items()}


class SchemaIterator(object):
    """Recursively iterate over all columns in a table

    Iterates over all columns of a table including columns in referenced tables

    Args:
        schema: Schema
    """
    def __init__(self, schema: dict) -> None:
        self._schema = schema
        self._cols = list()
        self._tables = list()
        self._index = -1

    def __int__(self) -> int:
        return int(self._index)

    def __call__(self,
                 table: str,
                 depth: Optional[int] = -1) -> Generator["SchemaIterator",
                                                         None,
                                                         None]:
        """Iterate over all columns of the specified table up to a maximum depth

        Args:
            table: Name of table to iterate over
            depth: Recursion depth. Use -1 for infinite recursion. Defaults to
                -1.

        Yields:
            One :class:`~fsgop.db.SchemaIterator` for each column in `table`
        """
        self._cols.clear()
        self._tables.clear()
        self._index = -1
        yield from self._iter(table, depth=depth)

    @property
    def table(self) -> TableInfo:
        """Access current table"""
        return self._tables[-1]

    @property
    def column(self) -> ColumnInfo:
        """Access current column"""
        return self._cols[-1]

    @property
    def path(self) -> Tuple[str]:
        """Get name of this column and all parent columns"""
        return tuple(col.name for col in self._cols)

    @property
    def unique_table_name(self) -> str:
        """Get unique name for the current table"""
        return "_".join(self.path[:-1] + (self.table.name,))

    @property
    def unique_name(self) -> str:
        """Get unique name for the current column"""
        return f"{self.unique_table_name}.{self.column.name}"

    def parent(self, unique: Optional[bool] = True) -> Optional[str]:
        """Get name of table and column, by which this column is referenced

        Args:
            unique: If ``True``, unique names are returned.

        Returns:
            name of parent column referencing the current column, ``None`` if the
            current column is not referenced by its parent.
        """
        retval = None
        if len(self._cols) < 2:
            return retval
        table, col = self._tables[-2], self._cols[-2]
        if col.ref_info == (self.table.name, self.column.name):
            if unique:
                tname = "_".join(self.path[:-2] + (table.name,))
                retval = f"{tname}.{col.name}"
            else:
                retval = f"{table.name}.{col.name}"
        return retval

    def _iter(self,
              table: str,
              depth: int = -1) -> Generator["SchemaIterator", None, None]:
        """Iteration implementation

        Args:
            table: Table name
            depth: Max. recursion depth

        Yields:
            One :class:`~fsgop.db.SchemaIterator` for each column in `table`
        """
        self._tables.append(self._schema[table])
        self._cols.append(None)
        for self._cols[-1] in self.table:
            self._index += 1
            ref_table, ref_col = self.column.ref_info
            if ref_table is not None and depth != 0:
                yield from self._iter(ref_table, depth=depth - 1)
            else:
                yield self
        self._cols.pop()
        self._tables.pop()
        return
