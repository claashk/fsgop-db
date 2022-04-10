import logging
from typing import Optional, Generator, Iterable, NamedTuple, Any, Union
from pathlib import Path
from tempfile import TemporaryDirectory
from tarfile import TarFile
from collections import namedtuple

from .table_info import TableInfo, IndexInfo, sort_tables, to_schema
from .table_info import SchemaIterator, dependencies
from .table_io import CsvParser

logger = logging.getLogger(__name__)


class Database(object):
    """Base class implementing a generic database interface

    Args:
        db: Name of Database to open. Defaults to ``None``
        schema: Dictionary describing the underlying schema of the database.
        **kwargs: Additional keyword arguments forwarded verbatim to
           :meth:`Database.connect`
    """
    placeholder = "?"
    placeholder_prefix = ":"
    placeholder_postfix = ""

    def __init__(self,
                 db: Optional[str] = None,
                 schema: Optional[dict] = None,
                 **kwargs) -> None:
        self._db = None
        self.schema = to_schema(schema) if schema is not None else None
        if db is not None:
            self.connect(db=db, schema=self.schema, **kwargs)
    
    def __enter__(self) -> "Database":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()

    def connect(self,
                db: Optional[str] = None,
                schema: Optional[dict] = None,
                **kwargs) -> None:
        """Connect to server. Has to be implemented by derived class.

        Args:
            db: Name of Database to open.
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

    def enable_foreign_key_checks(self):
        """Enable foreign key checks on this database"""
        raise NotImplementedError()

    def disable_foreign_key_checks(self):
        """Disable foreign key checks on this database"""
        raise NotImplementedError()

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

    def reset(self, schema: Optional[dict] = None) -> None:
        """Delete all tables and re-create empty database with new schema

        WARNING: Deletes all tables including their content!

        Args:
            schema: Schema dictionary
        """
        if schema is None:
            _schema = self.schema if self.schema is not None else dict()
        else:
            _schema = to_schema(schema)

        tables = self.list_tables()
        if tables:
            cursor = self._db.cursor()
            self.disable_foreign_key_checks()
            for table in tables:
                cursor.execute(f"DROP TABLE IF EXISTS '{table}'")
            self.enable_foreign_key_checks()

        for info in _schema.values():
            self.create_table(table_info=info)
        self.schema = _schema

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
        cursor = self._db.cursor()
        cursor.execute(f"CREATE TABLE{_force} {_name}({_cols}, {_key})")

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
        cursor = self._db.cursor()
        cursor.execute("CREATE"
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
            rectype: Type returned by this class. Must be constructable
                from the columns in this class. If not specified, a namedtuple
                will be constructed from the table info.
            **kwargs: Variables to be inserted into the `where` and `order`
                strings in a safe manner.
            
        Yields:
            Matching table records
        """
        table = self.schema[name]
        _where = f" WHERE {where}" if where is not None else ""
        _order = f" ORDER BY {order}" if order else ""
        _type = table.record_type if rectype is None else rectype
        cursor = self._db.cursor()
        cursor.execute(f"SELECT * FROM {table.name}{_where}{_order}", kwargs)
        for row in cursor:
            yield _type(*row)

    def join(self,
             table: str,
             where: Optional[str] = None,
             order: Optional[str] = None,
             depth: int = -1,
             **kwargs) -> Generator[NamedTuple, None, None]:
        """Join records of a table

        Joins all referencing columns with the associated tables and yields
        the resulting records

        Args:
            table: Name of table to query
            where: Search string passed verbatim to WHERE statement of SELECT
            order: String passed verbatim to ORDER statement
            depth: Maximum recursion depth. -1 implies infinite recursion.
                Defaults to -1.
            **kwargs: Variables to be inserted into the `where` and `order`
                strings in a safe manner.

        Yields:
            Joined records
        """
        traverse = SchemaIterator(self.schema)
        fields = []
        cols = []
        joins = []
        for it in traverse(table, depth=depth):
            fields.append("_".join(it.path))
            # we need unique table names to avoid name conflicts
            table_name = it.unique_table_name
            col_name = f"{table_name}.{it.column.name}"
            cols.append(col_name)
            ref = it.parent(unique=True)
            if ref is not None:
                joins.append(f"LEFT JOIN {it.table.name} {table_name} "
                             f"ON {col_name} = {ref}")

        _name = f"{''.join(s.capitalize() for s in table.split('_'))}"
        _type = namedtuple(f"Extended{_name}Record", fields)
        _cols = ",".join(cols)
        _joins = " ".join(joins)
        _where = f" WHERE {where}" if where is not None else ""
        _order = f" ORDER BY {order}" if order else ""

        cursor = self._db.cursor()
        cursor.execute(f"SELECT {_cols} FROM {table} {_joins}{_where}{_order}",
                       kwargs)
        for row in cursor:
            yield _type(*row)

    def count(self,
              name: str,
              where: Optional[str] = None,
              order: Optional[str] = None,
              **kwargs) -> Generator[NamedTuple, None, None]:
        """Count records matching criteria

        Args:
            name: Name of table to analyse. Must be a valid key into the schema
                dictionary.
            where: Any filter string in the format passed to *SQL* ``WHERE``
                command. If *None*, no filter is applied. Defaults to ``None``.
                Use escaped code for variables and pass the variables as keyword
                arguments to avoid SQL injection.
            order: Optional order key. Passed verbatim to *SQL* `ORDER BY`
                statement. Defaults to ``None``.
            **kwargs: Variables to be inserted into the `where` and `order`
                strings in a safe manner.

        Yields:
            Matching table records
        """
        table = self.schema[name]
        where_str = f" WHERE {where}" if where is not None else ""
        order_str = f" ORDER BY {order}" if order else ""
        cursor = self._db.cursor()
        query = f"SELECT COUNT(*) FROM {table.name}{where_str}{order_str}"
        cursor.execute(query, kwargs)
        return cursor.fetchone()[0]

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
        command = str(f"{'REPLACE' if force else 'INSERT OR IGNORE'} "
                      f"INTO {table.name} "
                      f"VALUES {table.format(self.placeholder)}")
        cursor = self._db.cursor()
        cursor.executemany(command, rows)

    def delete(self,
               name: str,
               where: Optional[str] = None,
               parameters: Optional[Iterable[Union[dict, tuple]]] = None,
               **kwargs) -> None:
        """Delete records from table                
        
        Args:
            name: Table name. Must be a valid key in the current schema dict.
            where: Passed verbatim to *SQL*'s ``WHERE`` clause to identify rows
                to delete. If ``None`` or the empty string, all rows will be
                deleted. Defaults to ``None``.
            parameters: A sequence of parameters for parameterized deletes. If
                specified, the where string needs to use a parameterized query
                and the sequence shall yield the associated parameter sets.
                No keyword arguments are allowed if sequence is specified.
            **kwargs: Variables inserted into the where string.
        """
        _where = f" WHERE {where}" if where is not None else ""
        query = f"DELETE FROM {name}{_where}"
        cursor = self._db.cursor()
        if parameters is None:
            cursor.execute(query, kwargs)
        else:
            assert not kwargs
            cursor.executemany(query, parameters)

    def delete_ids(self, name: str, ids: Iterable[int]) -> None:
        """Delete records by id
        
        Args:
            name: Table name. Must be a valid key in the current schema dict.
            ids: ids to be deleted. Each element should be convertible to an
                integer.
        """
        id_col = self.schema[name].id_column
        self.delete(name,
                    where=f"{id_col}={self.var('uid')}",
                    parameters=({"uid": int(x)} for x in ids))

    def update(self,
               name: str,
               assignment: str,
               where: Optional[str] = None,
               parameters: Optional[Iterable[Union[dict, tuple]]] = None,
               **kwargs) -> None:
        """Update value in table
        
        Uses mysql ``UPDATE`` statement to update values of a table.
        
        Args:
            name: Table name
            assignment: Update information in format compatible with MySQL
               ``SET`` clause of ``UPDATE`` statement
            where: Optional filter string passed verbatim to ``WHERE`` statement.
            parameters: A sequence of parameters for parameterized updates. If
                specified, the *where* string needs to use a parameterized query
                and the sequence shall yield the associated parameter sets.
                No other keyword-based variables are allowed in combination with
                this argument.
            **kwargs: Variables to be inserted into where string.

        Example:
            Assuming *db* is a connected :class:`~fsgop.db.Database` instance,
            the following code replaces each occurrence of the ``pilot_id`` 3
            with a ``pilot_id`` of 5 in table *Flights*:
        
            .. code-block:: python
            
               import pysk.db.model.Flight as Flight
               [...]
               db.update("Mission", "pilot_uid=5", where="pilot_uid=3")
        """
        table = self.schema[name]
        _where = f" WHERE {where}" if where else ""
        query = f"UPDATE {table.name} SET {assignment}{_where}"
        cursor = self._db.cursor()
        if parameters is None:
            cursor.execute(query, kwargs)
        else:
            assert not kwargs
            cursor.executemany(query, parameters)

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
            raise KeyError(f"Found no result matching '{where}' (with {kwargs})")
        return retval

    def unique_id(self,
                  name: str,
                  uid: int,
                  rectype: Optional[Any] = None) -> NamedTuple:
        """Get unique result of a query
        
        Args:
            name: Table name
            uid: ID of item to select
            rectype: Record type. Passed verbatim to meth:`Database.select`
        
        Return:
            Record with specified uid

        Raises:
            KeyError if no unique matching record can be found
        """
        id_col = self.schema[name].id_column
        return self.unique(name,
                           where=f"{id_col}={int(uid)}",
                           rectype=rectype)

    def replace(self, name: str, where: str, by: int) -> None:
        """Replace records with another existing record

        Args:
            name: Name of table in which to replace records
            where: Search string defining the records to be replaced. All records
                in table *name* matching this query are replaced.
            by: UID of record used as replacement
        """
        uid = int(by)
        id_col = self.schema[name].id_column
        uids = []
        new_rec = self.unique_id(name, uid)
        for rec in self.select(name, where=where):
            for local_col, ref_table, ref_col in dependencies(self.schema, name):
                self.update(ref_table,
                            f"{ref_col}='{getattr(new_rec, local_col)}'",
                            where=f"{ref_col}='{getattr(rec, local_col)}'")
                uids.append(getattr(rec, id_col))
        if uid in uids:
            uids.remove(uid)
        self.delete_ids(name, uids)

    def max_id(self, name: str) -> int:
        """Get current maximum ID (beware of race conditions)

        Args:
             name: Table name

        Return:
            Maximum ID used in table 'name', -1 if no records are used
        """
        col = self.schema[name].id_column
        if col is None:
            raise ValueError(f"Table {name} has no unique ID column")
        # query is safe here, since an invalid name will raise in schema lookup
        cursor = self._db.cursor()
        cursor.execute(f"SELECT MAX({col}) from {name}")
        for rec in cursor.fetchall():
            return -1 if rec[0] is None else int(rec[0])
        raise ValueError("Unable to retrieve maximum of column")

    @classmethod
    def var(cls, name: str) -> str:
        """Get variable as named placeholder

        Args:
            name: Variable name

        Return:
            Properly escaped string to use `name` as variable in database query
        """
        return f"{cls.placeholder_prefix}{name}{cls.placeholder_postfix}"

    @classmethod
    def from_dump(cls,
                  path: Union[str, Path],
                  schema: dict,
                  db: str) -> "Database":
        """Create database from ASCII dump files as created by mysql dump

        Args:
            path: Path to directory or compressed archive containing the dump
                files. The dump files shall have the table name as basename and
                suffix `.txt`.
            schema: Dictionary describing the expected schema of the database to
                create.
            db: Name of database to create. If a database with this name
                already exists, it will be overwritten.
        Return:
            Database: Database created from dump files
        """
        src = Path(path)
        if src.is_file() and "".join(src.suffixes) in (".tar.gz", ".tgz"):
            archive = TarFile.open(src, mode="r:gz")
            with TemporaryDirectory() as tmpdir:
                archive.extractall(path=tmpdir)
                dest = Path(tmpdir) / src.stem
                return cls.from_dump(dest, schema, db)

        if not src.is_dir():
            raise ValueError(f"{src} is not a directory")

        database = cls(db)
        reader = CsvParser()
        table_src = ""
        try:
            database.reset(schema)
            for table in sort_tables(database.schema.values()):
                table_src = src / Path(table.name).with_suffix(".txt")
                logger.info(f"Importing {table_src} ...")
                database.insert(table.name,
                                table.read_mysql_dump(table_src, reader=reader))
        except:
            database.disconnect()
            logger.error(f"In line {reader.line_number} of {table_src}:\n")
            raise
        database.commit()
        return database
