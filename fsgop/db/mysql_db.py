from typing import Optional, List, Iterable
import MySQLdb as mysql
from .database import Database
from .table_info import TableInfo, ColumnInfo, IndexInfo


class MysqlDatabase(Database):
    """MySql Database implementation

    Args:
        db: Name of Database to open.
        schema: Database schema to use
        **kwargs: Keyword arguments passed verbatim to
            :meth:`MysqlDatabase.connect`
    """
    placeholder = "%s"
    placeholder_prefix = "%("
    placeholder_postfix = ")"

    def __init__(self,
                 db: Optional[str] = None,
                 schema: Optional[dict] = None,
                 **kwargs) -> None:
        super().__init__(db=db, schema=schema, **kwargs)

    def connect(self,
                db: Optional[str] = None,
                schema: Optional[dict] = None,
                host: str = "localhost",
                user: str = "user",
                password: Optional[str] = None) -> None:
        """Connect to MySQL server
        
        Args:
            db: Name of Database to open. Defaults to ``None``.
            schema: Database schema to use. Defaults to ``None``.
            host: Hostname. Defaults to ``'localhost'``.
            user: MySQL username. Defaults to ``'user'``.
            password: Password for user. Defaults to ``None``.
        """
        self._db = mysql.connect(host, user, password, db)
        self._cursor = self._db.cursor()
        self.enable_foreign_key_checks()
        if self.schema is None:
            self.schema = self.get_schema()

    def enable_foreign_key_checks(self):
        """Enable foreign key checks on this database"""
        self._cursor.execute("SET foreign_key_checks = 1")

    def disable_foreign_key_checks(self):
        """Disable foreign key checks on this database"""
        self._cursor.execute("SET foreign_key_checks = 0")

    def list_tables(self) -> List[str]:
        """Get list of table names
                    
        Returns:
            List of strings containing the table names
        """
        self._cursor.execute("SHOW TABLES")
        return [t[0] for t in self._cursor.fetchall()]

    def get_table_info(self, name: str) -> TableInfo:
        """Get information about a table

        Args:
            name: Table name. This is not translated with the current schema.

        Returns:
            Table information
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

    def get_references(self, table: str, column: str) -> Optional[str]:
        """Get column referenced by column in another table
        """
        self._cursor.execute("SELECT database()")
        db_name = "".join(rec[0] for rec in self._cursor.fetchall())
        self._cursor.execute("SELECT REFERENCED_TABLE_NAME, "
                             "REFERENCED_COLUMN_NAME "
                             "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
                             f"WHERE REFERENCED_TABLE_SCHEMA = '{db_name}' "
                             f"AND TABLE_NAME = '{table}' "
                             f"AND COLUMN_NAME = '{column}'")
        ref = list(self._cursor.fetchall())
        if not ref:
            return None

        if len(ref) > 1:
            raise RuntimeError(f"Expected at most one column, found {len(ref)}")

        return f"{ref[0][0]}({ref[0][1]})"

    def get_index_info(self, name: str) -> Iterable[IndexInfo]:
        """Get information about the indices of a table

        Args:
            name: Name of the table. This is not translated via current schema.

        Returns:
            List containing one :class:`fsgop.db.IndexInfo` object per index
        """
        order = {"A": 1, "D": -1, "NULL": 0}
        indices = dict()
        self._cursor.execute(f"SHOW INDEX FROM `{name}`")
        for rec in self._cursor.fetchall():
            idx = indices.setdefault(
                rec[2],
                IndexInfo(name=rec[2],
                          is_unique=(int(rec[1]) == 0),
                          is_primary=(rec[2].upper()) == "PRIMARY"))
            idx.add_column(name=rec[4],
                           order=order[rec[5]],
                           sequence=int(rec[3]) - 1)
        return indices.values()
