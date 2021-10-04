

class Database(object):
    """Generic database implementation

    If *password* is not ``None``, a new connection to the database will be
    attempted.        
        
    Arguments:
        host (str): Hostname. Defaults to '*localhost*'
        user (str): MySQL username. Defaults to '*startkladde*'.
        password (str): Password for user. Defaults to ``None``.
        database (str): Name of Database to open. Defaults to '*startkladde*'.        
    """
    def __init__(self, database=None, schema=None, **kwargs):
        self._db = None
        self._cursor = None
        self.schema = None
        if database is not None:
            self.connect(database=database, schema=schema, **kwargs)
    
    def connect(self, database, schema=None, **kwargs):
        """Connect to MySQL server
        
        Arguments:
            host (str): Hostname. Defaults to '*localhost*'.
            user (str): MySQL username. Defaults to '*startkladde*'.
            password (str): Password for user. Defaults to ``None``.
            database (str): Name of Database to open. Defaults to '*startkladde*'.
        """
        raise NotImplementedError()

    def disconnect(self):
        """Disconnect from database"""
        self._db.close()
        
    def commit(self):
        """Commit all changes to the database
        """
        self._db.commit()

    def list_tables(self):
        """Get list of table names
                    
        Return:
            list: List of strings containing the table names
        """
        raise NotImplementedError()

    def get_table_info(self, table):
        """Get information about a table

        Arguments:
            table (str): Name of the table

        Return:
            TableInfo: Information about the table
        """
        raise NotImplementedError()

    def get_schema(self):
        tables = self.list_tables()
        schema = dict()

        for name in tables:
            schema[name] = self.get_table_info(name)
        return schema

    def create_table(self, table, force=False):
        _force = "" if force else " IF NOT EXISTS"
        _cols = ",".join(f"{col.name} {col.dtype}"
                         f"{'' if col.allows_null else ' NOT NULL'}"
                         f"{' PRIMARY KEY' if col.is_primary_index() else ''}"
                         f"DEFAULT {col.default()}"
                         for col in table)
        self._cursor.execute(f"CREATE TABLE{_force} {table.name}({_cols})")

    def export_schema(self):
        if self.schema is None:
            return None
        return {k: v.export() for k, v in self.schema.items()}

    def select(self, name, where=None, order=None):
        """Iterate over the rows of a given table                
        
        Arguments:
            name(str): Name of table to select data from. Must be a valid key
               into the schema dictionary.
            where (str): Any filter string in the format passed to *SQL*
               ``WHERE`` command. If *None*, no filter is applied. Defaults to
               *None*.
            order (str): Optional order key. Passed verbatim to *SQL*
               ``ORDER BY`` statement. Defaults to *None*.
            
        Yields:
            RecordType: Table record
        """
        table = self.schema[name]
        where_str = f" WHERE {where}" if where is not None else ""
        order_str = f" ORDER BY {order}" if order else ""
        self._cursor.execute(f"SELECT * FROM {table.name}{where_str}{order_str}")
        RecordType = table.record_type("RecordType")
        for row in self._cursor:
            yield RecordType(*row)

    def insert(self, name, rows, force=False):
        """Insert new values into a table                
        
        Arguments:
             name (str): Name of table to insert into. Must be a valid key of
                the current schema.
             rows (iterable): Rows to insert into the table. Each row shall be
                provided in terms of a tuple with one element per column in
                table 'name'.
            force (bool): If True, existing rows are overwritten. Otherwise they
               are ignored. Defaults to False.
        """
        table = self.schema[name]
        fmt = f"({','.join(table.ncols * ['{}'])})"
        command = str(f"{'REPLACE' if force else 'INSERT IGNORE'} "
                      f"INTO {table.name} VALUES {fmt}")
        
        for row in rows:
            self._cursor.execute(command.format(*row))

    def delete(self, name, where=None):
        """Delete records from table                
        
        Arguments:
            name (str): Table name. Must be a valid key in the current schema
               dict.
            where (str): Passed verbatim to *SQL*'s ``WHERE`` clause
               to identify the rows to be deleted. If filter is *None* or the
               empty string, all rows will be deleted. Defaults to *None*.
        """
        _where = " WHERE {where}" if filter is not None else ""
        self._cursor.execute(f"DELETE FROM {name}{_where}")

    def delete_ids(self, name, ids):
        """Delete records by id
        
        Arguments:
            name (str): Table name. Must be a valid key in the current schema
               dict.
            ids (iterable): ids to be deleted. Each element should be
               convertible to an integer.
        """
        id_col = self.schema[name].columns[0]
        if ids:
            self.delete(name, where=f"{id_col} IN ({','.join(map(str, ids))}")

    def update(self, name, assignment, where=None):
        """Update value in table
        
        Uses mysql ``UPDATE`` statement to update values of a table.
        
        Arguments:
            name (str): Table name
            assignment (str): Update information in format compatible with MySQL
               ``SET`` clause of ``UPDATE`` statement
            where (str): Optional filter string passed verbatim to ``WHERE``
               statement.

        Example:
            Assuming *db* is a connected :class:`.db.Database` instance, the
            following code replaces each occurence of the ``pilot_id`` 3 with a
            ``pilot_id`` of 5 in table *Flights*:
        
            .. code-block:: python
            
               import pysk.db.model.Flight as Flight
               [...]
               db.update(Flight, "pilot_id=5", "pilot_id=3")
        """
        table = self.schema[name]
        _where = f" WHERE {where}" if where else ""
        self._cursor.execute(f"UPDATE {table.name} SET {assignment}{_where}")

    def unique(self, name, where):
        """Get unique result of a query
        
        Arguments:
            name (str): Table name
            where (str): Filter criteria
        
        Return:
            Instance of *cls* matching query, if and only if the query returns
            exactly one result. Otherwise a :class:`KeyError` is raised.
        """        
        retval = None
        for result in self.select(name, where=where):
            if retval is None:
                retval = result
            else:
                raise KeyError(f"Found more than one result matching '{where}'")

        if retval is None:
            raise KeyError(f"Found no result matching '{where}'")
        return retval

    def unique_id(self, name, uid):
        """Get unique result of a query
        
        Arguments:
            name (str): Class to select
            uid (int): ID of item to select
        
        Return:
            Instance of *cls* matching query, if and only if the query returns
            exactly one result. Otherwise a :class:`KeyError` is raised.
        """
        id_col = self.schema[name].columns[0]
        if id:
            return self.unique(name, where=f"{id_col}={uid}")
        return None
