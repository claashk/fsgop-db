import sqlite3
from .database import Database
from .table_info import TableInfo, ColumnInfo, IndexInfo


class SqliteDatabase(Database):
    """MySql Database implementation

    Arguments:
        host (str): Hostname. Defaults to '*localhost*'
        user (str): MySQL username. Defaults to '*startkladde*'.
        password (str): Password for user. Defaults to ``None``.
        database (str): Name of Database to open. Defaults to '*startkladde*'.        
    """
    def __init__(self, database=None, schema=None, **kwargs):
        super().__init__(database=database, schema=schema, **kwargs)

    def connect(self, database, schema=None, **kwargs):
        """Connect to MySQL server
        
        Arguments:
            host (str): Hostname. Defaults to '*localhost*'.
            user (str): MySQL username. Defaults to '*startkladde*'.
            password (str): Password for user. Defaults to ``None``.
            database (str): Name of Database to open. Defaults to '*startkladde*'.
        """
        self._db = sqlite3.connect(database, **kwargs)
        self._cursor = self._db.cursor()
        self.schema = self.get_schema() if schema is None else dict(schema)

    def list_tables(self):
        """Get list of table names
                    
        Return:
            list: List of strings containing the table names
        """
        self._cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [t[0] for t in self._cursor.fetchall()]

    def get_index_info(self, name):
        indices = []
        self._cursor.execute(f"PRAGMA index_list({name})")
        for rec in self._cursor.fetchall():
            indices.append(IndexInfo(name=rec[1],
                                     is_unique=(int(rec[2]) == 1),
                                     is_primary=(rec[3] == "pk")))
        for idx in indices:
            self._cursor.execute(f"PRAGMA index_xinfo({idx.name})")
            for rec in self._cursor.fetchall():
                if int(rec[5]) == 0:  # -> aux column
                    continue
                idx.add_column(name=rec[2],
                               order=-1 if int(rec[3]) == 1 else 1,
                               sequence=rec[0])
        return indices

    def get_table_info(self, name):
        table = TableInfo(name=name, indices=self.get_index_info(name))
        self._cursor.execute(f"PRAGMA table_info('{name}')")
        for rec in self._cursor.fetchall():
            default_value = str(rec[4])
            table.add_column(ColumnInfo(
                name=rec[1],
                dtype=rec[2],
                allows_null=(int(rec[3]) == 0),
                index="PRI" if int(rec[5]) else "",
                default_value=None if default_value.upper() == "NULL"
                                   else default_value))
        return table
