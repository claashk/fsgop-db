from typing import Optional, Generator, Iterable, Callable, NamedTuple, Any
from datetime import datetime
from .table_info import TableInfo
from .table_info import IndexInfo


class Database(object):
    """Base class implementing a generic database interface

    Args:
        database: Name of Database to open. Defaults to ``None``
        schema: Dictionary describing the underlying schema of the database.
        **kwargs: Additional keyword arguments forwarded verbatim to
           :meth:`Database.connect`
    """
    date_format = "%Y-%m-%d"
    time_format = "%H:%M"
    datetime_format = f"{date_format} {time_format}"
    placeholder = "?"
    placeholder_prefix = ":"
    placeholder_postfix = ""

    def __init__(self,
                 database: Optional[str] = None,
                 schema: Optional[dict] = None,
                 **kwargs) -> None:
        self._db = None
        self._cursor = None
        self.schema = dict(schema) if schema is not None else None
        if database is not None:
            self.connect(database=database, schema=schema, **kwargs)
    
    def connect(self,
                database: Optional[str] = None,
                schema: Optional[dict] = None,
                **kwargs) -> None:
        """Connect to server. Has to be implemented by derived class.

        Args:
            database: Name of Database to open.
            schema: Schema
            **kwargs: Additional keyword arguments
        """
        raise NotImplementedError()

    def disconnect(self) -> None:
        """Disconnect from database"""
        self._db.close()
        
    def commit(self) -> None:
        """Commit all changes to the database
        """
        self._db.commit()

    def list_tables(self) -> list:
        """Get list of table names. Has to be implemented by derived class.
                    
        Returns:
            List of strings containing the table names
        """
        raise NotImplementedError()

    def get_table_info(self, table: str) -> TableInfo:
        """Get information about a table. Has to be implemented by derived class

        Args:
            table: Name of the table

        Return:
            TableInfo: Information about the table
        """
        raise NotImplementedError()

    def get_schema(self) -> dict:
        """Get schema of this database

        Returns:
            Dictionary describing this table
        """
        tables = self.list_tables()
        schema = dict()

        for name in tables:
            schema[name] = self.get_table_info(name)
        return schema

    def create_table(self, table_info: TableInfo, force: bool = False):
        """Create a new table
        
        Args:
            table_info: Table information
            force: If ``True``, existing tables are overwritten without warning.
        """
        _force = "" if force else " IF NOT EXISTS"
        _name = table_info.name
        _cols = ",".join(f"{col.name} {col.dtype}"
                         f"{'' if col.allows_null else ' NOT NULL'} "
                         f"DEFAULT {col.default()}"
                         f"{col.sql_references()}"
                         for col in table_info)
        _key = table_info.primary_key()
        self._cursor.execute(f"CREATE TABLE{_force} {_name}({_cols}, {_key})")

        for name, idx in table_info.indices():
            if idx.is_primary:
                continue
            self.create_index_for_table(_name, idx)

    def create_index_for_table(self, name: str, index: IndexInfo) -> None:
        """Create index for a table

        Args:
            name: Table name
            index: Information about the index
        """
        self._cursor.execute("CREATE"
                             f"{' UNIQUE' if index.is_unique else ''} "
                             f" INDEX {index.name} ON {name} "
                             f"({index.key_format()})")

    def export_schema(self) -> Optional[dict]:
        """Export current schema

        Returns:
            Dictionary with schema information, ``None`` if no schema is set
        """
        if self.schema is None:
            return None
        return {k: v.as_dict() for k, v in self.schema.items()}

    def select(self,
               name: str,
               where: Optional[str] = None,
               order: Optional[str] = None,
               rectype: Optional[Any] = None,
               **kwargs) -> Generator[NamedTuple, None, None]:
        """Iterate over the rows of a given table                
        
        Args:
            name: Name of table to select data from. Must be a valid key into the
                schema dictionary.
            where: Any filter string in the format passed to *SQL* ``WHERE``
                command. If *None*, no filter is applied. Defaults to ``None``.
                Use escaped code for variables and pass the variables as keyword
                arguments to avoid SQL injection.
            order: Optional order key. Passed verbatim to *SQL* `ORDER BY`
                statement. Defaults to ``None``.
            rectype: Type returned by this class. Must be constructible
                from the columns in this class. If not specified, a namedtuple
                will be constructed from the table info.
            **kwargs: Variables to be inserted into the `where` and `order`
                strings in a safe manner.
            
        Yields:
            Matching table records
        """
        table = self.schema[name]
        where_str = f" WHERE {where}" if where is not None else ""
        order_str = f" ORDER BY {order}" if order else ""
        self._cursor.execute(f"SELECT * FROM {table.name}{where_str}{order_str}",
                             kwargs)
        Rec = table.record_type("Rec") if rectype is None else rectype
        for row in self._cursor:
            yield Rec(*row)

    def insert(self,
               name: str,
               rows: Iterable[tuple],
               force: bool = False) -> None:
        """Insert new values into a table                
        
        Args:
             name: Name of table to insert into. Must be a valid key of the
                 current schema.
             rows: Rows to insert into the table. Each row shall be provided as
                 tuple with one element per column in table 'name'.
             force: If ``True``, existing rows are overwritten. Otherwise they
               are ignored. Defaults to ``False``.
        """
        table = self.schema[name]
        command = str(f"{'REPLACE' if force else 'INSERT IGNORE'} "
                      f"INTO {table.name} "
                      f"VALUES {table.format(self.placeholder)}")
        self._cursor.executemany(command, rows)

    def delete(self, name: str, where: Optional[str] = None, **kwargs) -> None:
        """Delete records from table                
        
        Args:
            name: Table name. Must be a valid key in the current schema dict.
            where: Passed verbatim to *SQL*'s ``WHERE`` clause to identify rows
                to delete. If ``None`` or the empty string, all rows will be
                deleted. Defaults to ``None``.
            **kwargs: Variables inserted into the where string.
        """
        _where = f" WHERE {where}" if where is not None else ""
        self._cursor.execute(f"DELETE FROM {name}{_where}", kwargs)

    def delete_ids(self, name: str, ids: Iterable[int]) -> None:
        """Delete records by id
        
        Args:
            name: Table name. Must be a valid key in the current schema dict.
            ids: ids to be deleted. Each element should be convertible to an
                integer.
        """
        id_col = self.schema[name]._cols[0]
        if ids:
            idstr = ",".join(f"{int(x)}" for x in ids)
            self.delete(name, where=f"{id_col} IN ({idstr})")

    def update(self,
               name: str,
               assignment: str,
               where: Optional[str] = None,
               **kwargs) -> None:
        """Update value in table
        
        Uses mysql ``UPDATE`` statement to update values of a table.
        
        Args:
            name: Table name
            assignment: Update information in format compatible with MySQL
               ``SET`` clause of ``UPDATE`` statement
            where: Optional filter string passed verbatim to ``WHERE`` statement.
            **kwargs: Variables to be inserted into where string.

        Example:
            Assuming *db* is a connected :class:`.db.Database` instance, the
            following code replaces each occurence of the ``pilot_id`` 3 with a
            ``pilot_id`` of 5 in table *Flights*:
        
            .. code-block:: python
            
               import pysk.db.model.Flight as Flight
               [...]
               db.update("Mission", "pilot_uid=5", where="pilot_uid=3")
        """
        table = self.schema[name]
        _where = f" WHERE {where}" if where else ""
        self._cursor.execute(f"UPDATE {table.name} SET {assignment}{_where}",
                             kwargs)

    def unique(self, name: str, where: str, **kwargs) -> NamedTuple:
        """Get unique result of a query
        
        Args:
            name: Table name
            where: Filter criteria
            **kwargs: Keyword arguments passed verbatim to
                :meth:`Database.select`.

        Return:
            Record matching query, if and only if the query returns exactly one
            result.

        Raises:
            KeyError if no unique matching record can be found
        """        
        retval = None
        for result in self.select(name, where=where, **kwargs):
            if retval is not None:
                raise KeyError(f"Found more than one record matching '{where}'")
            retval = result

        if retval is None:
            raise KeyError(f"Found no result matching '{where}'")
        return retval

    def unique_id(self, name: str, uid: int) -> NamedTuple:
        """Get unique result of a query
        
        Args:
            name: Table name
            uid: ID of item to select
        
        Return:
            Record with specified uid

        Raises:
            KeyError if no unique matching record can be found
        """
        id_col = self.schema[name]._cols[0]
        return self.unique(name, where=f"{id_col}={int(uid)}")

    def get_parser(self, dtype: str) -> Callable:
        """Get default parser for data types in this database

        Args:
            dtype: String identifying data type as listed in schema

        Returns:
            Function yielding correct datatype when applied to a string
        """
        s = dtype.lower()
        if "int" in s:
            return lambda x: int(x) if x != r"\N" else None
        if "real" in s or "floa" in s or "doub" in s:
            return lambda x: float(x) if x != r"\N" else None
        if s == "date":
            return(lambda x: datetime.strptime(x, self.date_format).date()
                   if x != r"\N" else None)
        if s == "datetime":
            return (lambda x: datetime.strptime(x, self.datetime_format)
                    if x != r"\N" else None)
        return lambda x: str(x) if x != r"\N" else None

    @classmethod
    def var(cls, name: str) -> str:
        """Get variable as named placeholder

        Args:
            name: Variable name

        Return:
            Properly escaped string to use `name` as variable in database query
        """
        return f"{cls.placeholder_prefix}{name}{cls.placeholder_postfix}"
