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
        
    def begin_transaction(self) -> None:
        """Begin a new manual transaction"""
        cursor = self._db.cursor()
        cursor.execute("BEGIN")

    def commit(self) -> None:
        """Commit all changes to the database
        """
        self._db.commit()

    def rollback(self) -> None:
        """Rollback the current transaction"""
        cursor = self._db.cursor()
        cursor.execute("ROLLBACK")

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
            # use to_schema to create a deep copy -> delete modifies self.schema
            _schema = dict() if self.schema is None else to_schema(self.schema)
        else:
            _schema = to_schema(schema)

        tables = self.list_tables()
        if tables:
            self.disable_foreign_key_checks()
            for table in tables:
                self.delete_table(table)
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
        logger.debug(f"Creating table '{_name}'{_force} ...")
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
        self.schema[_name] = table_info

    def delete_table(self, table: str) -> None:
        """Delete a table

        Args:
            table: Table name
        """
        logger.debug(f"Deleting table '{table}' ...")
        cursor = self._db.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS '{table}'")
        self.schema.pop(table, None)

    def rename_table_to(self, name: str, table: str) -> None:
        """Rename a table

        Args:
            name: New name of the table
            table: Current name of the table
        """
        logger.debug(f"Renaming table '{table}' to '{name}' ...")
        cursor = self._db.cursor()
        cursor.execute(f"ALTER TABLE '{table}' RENAME TO '{name}'")
        self.schema = self.get_schema()  # update schema -> references may change

    def create_index_for_table(self, name: str, index: IndexInfo) -> None:
        """Create index for a table

        Args:
            name: Table name
            index: Information about the index
        """
        logger.debug(f"Creating index '{index.name}' for table {name} ...")
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
                this argument. In case parameters are used in both where and
                assignment strings, the assignment string parameters precede the
                where string parameters.
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

    def replace(self, name: str, where: str, by: Optional[int] = None) -> None:
        """Replace records with another existing record

        Args:
            name: Name of table in which to replace records
            where: Search string defining the records to be replaced. All records
                in table *name* matching this query are replaced.
            by: UID of record used as replacement. If ``None``, NULL will be used
                as replacement.
        """
        id_col = self.schema[name].id_column
        if by is not None:
            uid = int(by)
            new_rec = self.unique_id(name, uid)
        else:
            uid = None
            new_rec = None

        uids = []
        for rec in self.select(name, where=where):
            for local_col, ref_table, ref_col in dependencies(self.schema, name):
                if new_rec is not None:
                    _with = f"{ref_col}='{getattr(new_rec, local_col)}'"
                else:
                    _with = f"{ref_col}=NULL"
                self.update(ref_table,
                            _with,
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

    def migrate_to(self,
                   schema: dict,
                   rename: Optional[dict] = None,
                   prefix="_new") -> "MigrationContext":
        """Migrate database

        Use as context manager

        Args:
            schema: Schema to migrate to
            rename: Dictionary containing tables which shall be just renamed
            prefix: Prefix used for temporary tables

        Return:
            migration context for the migration of individual tables
        """
        return MigrationContext(db=self,
                                schema=schema,
                                rename=rename,
                                prefix=prefix)

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


class MigrationContext:
    """A context manager for migrations

    Roughly follows the concept described here
    https://www.sqlite.org/lang_altertable.html#otheralter

    Args:
        db: Database to migrate
        schema: New schema to which to migrate
        rename: Dictionary containing names of current and new names. Tables
            in this dictionary will be renamed in first step of migration
        prefix: Prefix used to name temporary new tables. Defaults to ``'new_'``
    """
    def __init__(self,
                 db: Database,
                 schema: dict,
                 rename: Optional[dict] = None,
                 prefix: str = "new_") -> None:
        self.db = db
        self._rename = dict(rename) if rename is not None else dict()
        self._new_schema = to_schema(schema)
        self._prefix = str(prefix)

    def __enter__(self) -> "MigrationContext":
        logger.info("Beginning database migration ...")
        self.db.disable_foreign_key_checks()
        self.db.begin_transaction()

        # First rename tables, which are just renamed and not modified otherwise
        for new_name, name in self._rename.items():
            self.db.rename_table_to(new_name, name)
        self._create_new_tables()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        exception = None
        if exc_type is None:
            try:
                self._delete_tables()
            except Exception as ex:
                exception = ex
                exc_type = type(ex)

        if exc_type is None:
            self.db.commit()
            logger.info("Database migration complete")
        else:
            logger.warning("Database migration failed! Rolling back ...")
            self.db.rollback()

        self.db.enable_foreign_key_checks()
        if exception is not None:
            raise exception

    def migrate_to(self, name: str, table: str, converter=None):
        """Migrate a table to another table

        Copy all data from table ``table`` to table ``f'{self._prefix}{name}'``

        Args:
            name: Name of the destination table (without leading prefix).
            table: Name of table to migrate from (migration source).
            converter: A unary function accepting a record of the source table
                as input and returning a record of the destination table. If
                ``None``, a default migration object will be created, which
                copies fields by name and initializes new fields with ``None``.
        """
        logger.debug(f"Migrating table '{table}' to table '{name}' ...")
        tmp_name = f"{self._prefix}{name}"
        if converter is None:
            _type = self.db.schema[tmp_name].record_type
            _fields = _type._fields
            _conv = lambda rec: _type(**{k: getattr(rec, k, None)
                                         for k in _fields})
        else:
            _conv = converter

        self.db.insert(tmp_name,
                       (_conv(rec) for rec in self.db.select(table)),
                       force=True)
        self.db.delete_table(table)
        self.db.rename_table_to(name, tmp_name)

    def _create_new_tables(self):
        """Prepare for a database migration

        Iterates over all tables in the current schema and renames them to
        current name appended by postfix unless a table with exactly the same
        properties exists in ``schema``.

        Args:
            schema: Dictionary containing the new schema after the migration
            rename: Dictionary containing a name in the old dictionary as key
                associated with the name of a new table name.
            postfix: Postfix to add to all tables which have to be modified
                during migration.
        """
        for name, new_table_info in self._new_schema.items():
            try:
                table_info = self.db.schema[name]
                if new_table_info == table_info:
                    logger.debug(f"Keeping table '{name}' without changes")
                else:
                    # create a copy since we modify the name
                    tmp = TableInfo.from_list(
                        name=f"{self._prefix}{name}",
                        **new_table_info.as_dict())
                    self.db.create_table(tmp, force=False)

            except KeyError:
                self.db.create_table(new_table_info, force=False)

    def _delete_tables(self) -> None:
        """Finalise database migration

        Deletes all tables not found in the new schema

        Args:
            schema: Dictionary containing the new schema
            postfix: If not ``None`` tables are only deleted if their respective
                name ends in ``postfix``.
        """
        logger.info("Finalising database migration ...")
        logger.debug("Checking for unmigrated tables ...")
        tables = self.db.list_tables()
        missing = [t for t in tables if t.startswith(self._prefix)]
        if missing:
            s = "\n * ".join(missing)
            raise RuntimeError(f"Found {len(missing)} unmigrated tables:\n * {s}")

        for table in tables:
            if table not in self._new_schema.keys():
                self.db.delete_table(table)

        for table_info in self._new_schema.values():
            assert table_info == self.db.schema[table_info.name]
