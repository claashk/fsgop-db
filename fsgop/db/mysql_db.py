import MySQLdb as mysql
from .database import Database
from .table_info import TableInfo, ColumnInfo


class MysqlDatabase(Database):
    """MySql Database implementation

    Arguments:
        host (str): Hostname. Defaults to '*localhost*'
        user (str): MySQL username. Defaults to '*startkladde*'.
        password (str): Password for user. Defaults to ``None``.
        database (str): Name of Database to open. Defaults to '*startkladde*'.        
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
        self.schema = self.get_schema() if schema is None else dict(schema)

    def list_tables(self):
        """Get list of table names
                    
        Return:
            list: List of strings containing the table names
        """
        self._cursor.execute("SHOW TABLES")
        return [t[0] for t in self._cursor.fetchall()]

    def get_schema(self):
        tables = self.list_tables()
        schema = dict()

        for name in tables:
            self._cursor.execute(f"DESCRIBE `{name}`")
            table = schema.setdefault(name, TableInfo(name=name))
            for rec in self._cursor.fetchall():
                table.add_column(ColumnInfo(
                                     name=rec[0],
                                     dtype=rec[1],
                                     allows_null=(rec[2].upper() == "YES"),
                                     index=rec[3],
                                     default_value=rec[4],
                                     extra=rec[5].lower()))
        return schema
