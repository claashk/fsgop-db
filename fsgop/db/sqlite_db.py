from typing import Optional, List
import sqlite3
from .database import Database, to_schema
from .table_info import TableInfo, ColumnInfo, IndexInfo


class SqliteDatabase(Database):
    """Sqlite database implementation

    Args:
        db: Name of Database to open.
        schema: Schema in dictionary form.
    """
    def __init__(self,
                 db: Optional[str] = None,
                 schema: Optional[dict] = None,
                 **kwargs) -> None:
        super().__init__(db=db, schema=schema, **kwargs)

    def connect(self,
                db: Optional[str] = None,
                schema: Optional[dict] = None,
                **kwargs) -> None:
        """Connect to sqlite database
        
        Args:
            db: Name of Database to open.
            schema: Schema to use.
            **kwargs: Keyword arguments passed verbatim to ``sqlite3.connect``
        """
        self._db = sqlite3.connect(str(db), **kwargs)
        self.enable_foreign_key_checks()
        self.schema = self.get_schema() if schema is None else to_schema(schema)

    def enable_foreign_key_checks(self):
        """Enable foreign key checks on this database"""
        cursor = self._db.cursor()
        cursor.execute("PRAGMA foreign_keys = ON")

    def disable_foreign_key_checks(self):
        """Disable foreign key checks on this database"""
        cursor = self._db.cursor()
        cursor.execute("PRAGMA foreign_keys = OFF")

    def list_tables(self) -> List[str]:
        """Get list of table names
                    
        Returns:
            List of strings containing the table names
        """
        cursor = self._db.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [t[0] for t in cursor]

    def get_index_info(self, name: str) -> List[IndexInfo]:
        """Get indices for a given table

        Args:
            name: Table name

        Returns:
            One IndexInfo object per index
        """
        indices = []
        # The rowid column is not listed as an index. Tables without rowid column
        # are actually created as indices, so pragma_index_info returns something
        # Thus it is used here as test to check, if rowid exists or not.
        # See
        #   https://sqlite.org/forum/forumpost/90670c6dcb5e2125?t=h&unf
        cursor = self._db.cursor()
        cursor.execute("SELECT count(*) FROM pragma_index_info(:name)",
                       {"name": name})
        for c in cursor:  # -> loop has either one or no items
            if c[0] == 0:
                indices.append(IndexInfo(name="PRIMARY",
                                         is_unique=True,
                                         is_primary=True))

        cursor.execute(f"PRAGMA index_list({name})")
        for rec in cursor:
            indices.append(IndexInfo(name=rec[1],
                                     is_unique=(int(rec[2]) == 1),
                                     is_primary=(rec[3] == "pk")))
        for idx in indices:
            if idx.name == "PRIMARY" and idx.is_primary:
                # -> ROWID index, no index_xinfo
                cursor.execute("SELECT * FROM pragma_table_info(:name) "
                               "AS x where x.pk == 1;", {"name": name})
                for rec in cursor.fetchall():
                    idx.add_column(name=rec[1], order=1, sequence=0)
            else:
                cursor.execute(f"PRAGMA index_xinfo({idx.name})")
                for rec in cursor.fetchall():
                    if int(rec[5]) == 0:  # -> aux column
                        continue
                    idx.add_column(name=rec[2],
                                   order=-1 if int(rec[3]) == 1 else 1,
                                   sequence=rec[0])
        return indices

    def get_references(self, table: str, column: str) -> Optional[str]:
        """Get column referenced by a column in another table

        Args:
            table: Name of table containing referencing column
            column: Name of referencing column

        Returns:
            table name and column name referenced by the specified column.
            ``None`` if specified column does not refer to any other column.
        """
        cursor = self._db.cursor()
        cursor.execute(f"PRAGMA foreign_key_list('{table}')")
        ref = [(r[2], r[4]) for r in cursor if r[3] == column]
        if not ref:
            return None
        if len(ref) > 1:
            raise RuntimeError(f"Expected at most one column, found {len(ref)}")
        return f"{ref[0][0]}({ref[0][1]})"

    def get_table_info(self, name: str) -> TableInfo:
        """Get information about a table in this database

        Args:
            name: Table name

        Returns:
            TableInfo object
        """
        table = TableInfo(name=name, indices=self.get_index_info(name))
        cursor = self._db.cursor()
        cursor.execute(f"PRAGMA table_info('{name}')")
        for rec in cursor:
            default_value = str(rec[4])
            table.add_column(ColumnInfo(
                name=rec[1],
                dtype=rec[2],
                allows_null=(int(rec[3]) == 0),
                default_value=None if default_value.upper() == "NULL"
                              else default_value))
        for col in table:
            col.references = self.get_references(table.name, col.name)
        return table
