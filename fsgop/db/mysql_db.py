import MySQLdb as mysql
from .database import Database
from .table_info import TableInfo, ColumnInfo, IndexInfo
from .table_io import CsvParser


class MysqlDatabase(Database):
    """MySql Database implementation

    Arguments:
        database (str): Name of Database to open.
        schema (dict): Database schema to use
        **kwargs: Keyword arguments passed verbatim to
            :meth:`MysqlDatabase.connect`
    """
    def __init__(self, database=None, schema=None, **kwargs):
        super().__init__(database=database, schema=schema, **kwargs)

    def connect(self,
                database,
                schema=None,
                host="localhost",
                user="user",
                password=None):
        """Connect to MySQL server
        
        Arguments:
            host (str): Hostname. Defaults to '*localhost*'.
            user (str): MySQL username. Defaults to '*startkladde*'.
            password (str): Password for user. Defaults to ``None``.
            database (str): Name of Database to open. Defaults to '*startkladde*'.
        """
        self._db = mysql.connect(host, user, password, database)
        self._cursor = self._db.cursor()
        if self.schema is None:
            self.schema = self.get_schema()

    def list_tables(self):
        """Get list of table names
                    
        Return:
            list: List of strings containing the table names
        """
        self._cursor.execute("SHOW TABLES")
        return [t[0] for t in self._cursor.fetchall()]

    def get_table_info(self, name):
        """Get information about a table

        Arguments:
            name (str): Table name. This is not translated with the current
                schema.
        Return:
            fsgop.db.TableInfo: Table information
        """
        table = TableInfo(name=name, indices=self.get_index_info(name))
        self._cursor.execute(f"DESCRIBE `{name}`")
        for rec in self._cursor.fetchall():
            table.add_column(ColumnInfo(name=rec[0],
                                        dtype=rec[1],
                                        allows_null=(rec[2].upper() == "YES"),
                                        default_value=rec[4],
                                        extra=rec[5].lower()))
        for col in table:
            col.references = self.get_references(table.name, col.name)
        return table

    def get_references(self, table, column):
        self._cursor.execute("database()")
        db_name = self._cursor.fetchall()
        self._cursor.execute("SELECT REFERENCED_TABLE_NAME, "
                             "REFERENCED_COLUMN_NAME "
                             "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
                             f"WHERE REFERENCED_TABLE_SCHEMA = '{db_name}' "
                             f"AND TABLE_NAME = '{table}' "
                             f"AND COLUMN_NAME = '{column}'")
        ref = list(self._cursor.fetchall())
        if not ref:
            return ""

        if len(ref) > 1:
            raise RuntimeError(f"Expected at most one column, found {len(ref)}")

        return f"{ref[0][0]}({ref[0][1]})"

    def get_index_info(self, name):
        """Get information about the indices of a table

        Arguments:
            name (str): Name of the table. This is not translated via the
               current schema.
        Return:
            list: List containing one :class:`fsgop.db.IndexInfo` object per
            index
        """
        order = {"A": 1, "D": -1, "NULL": 0}
        indices = dict()
        self._cursor.execute(f"SHOW INDEX FROM `{name}`")
        for rec in self._cursor.fetchall():
            idx = indices.setdefault(
                rec[2],
                IndexInfo(name=rec[2],
                          is_unique=(int(rec[1] == 0)),
                          is_primary=(rec[2].upper()) == "PRIMARY"))
            idx.add_column(name=rec[4],
                           order=order[rec[5]],
                           sequence=int(rec[3]) - 1)
        return indices.values()

    def iter_dump_file(self, path, table=None, reader=None, aliases=None):
        """Import MySql dump file

        Arguments:
            path(str or pathlib.Path): Path to input file
            table (str): Name of table to import. If ``None``, table name is
                assumed to coincide with basename of file
            reader (:class:`fsgop.db.CsvParser`): reader. If ``None``, a new
               default reader will be constructed.
            aliases (dict or None): Dictionary containing a column name and an
               alias for this column name. The resulting namedtuple will use
               the aliases as column names. Names not found in aliases will
               remain unchanged.

        Yields:
            namedtuple: One namedtuple per record of the input file
        """
        if table is None:
            table = path.stem
        if reader is None:
            reader = CsvParser()
        table_info = self.schema[table]
        reader.row_type = table_info.record_type(aliases=aliases)
        parsers = table_info.record_parser(self.get_parser)

        for rec in reader(str(path), skip_rows=0, delimiter="\t"):
            yield reader.row_type(*tuple(p(x) for p, x in zip(parsers, rec)))
