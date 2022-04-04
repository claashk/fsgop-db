import logging
from typing import Iterable, Union, Generator, Optional, Callable
from pathlib import Path

from .person import Person, PersonProperty, Property
from .vehicle import Vehicle, VehicleProperty
from .mission import Mission
from .sqlite_db import SqliteDatabase
from .utils import Sequence, chunk, kwargs_from
from .record import Record
from . import startkladde as sk
from . native_schema import schema_v1

logger = logging.getLogger(__name__)


class Repository(object):
    """Repository implementation

    The repository is an intermediate layer between database and application (
    controllers, views). It wraps a database with a given schema and allows I/O
    operations using the native datamodel (Person, Vehicle, Mission, ...). This
    is the main distinction to the database, which works data model agnostic and
    uses only tuples and the native schema for I/O operations.

    The repository can be used as context manager.

    Args:
        db: Path to sqlite database file
    """
    native_types = {
        "people": Person,
        "vehicles": Vehicle,
        "person_properties": PersonProperty,
        "vehicle_properties": VehicleProperty,
        "missions": Mission
    }

    def __init__(self, db: Union[str, Path]) -> None:
        self._db = None
        self._native_tables = {v: k for k, v in self.native_types.items()}
        if db:
            self.open(db)

    def __enter__(self) -> "Repository":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def open(self, db: Union[str, Path], **kwargs) -> None:
        """Open a database file

        Args:
            db: Path to sqlite3 database file
            **kwargs: Keyword arguments passed verbatim to
                :class:`~fsgop.db.SqliteDatabase` constructor
        """
        self.close()
        self._db = SqliteDatabase(db=db, schema=schema_v1, **kwargs)

    def close(self):
        """Close the underlying database (file)"""
        if self._db is not None:
            self._db.disconnect()
            self._db = None

    def select(self,
               table: str,
               where: Optional[str] = None,
               order: Optional[str] = None,
               **kwargs) -> Generator[Record, None, None]:
        """Select records from a table

        Args:
            table: Name of table to select from
            where: SQL WHERE clause
            order: SQL ORDER BY clause
            **kwargs: Keyword arguments can be used to safely escape search
                parameters.

        Yields:
            Record instances matching the query.
        """
        _type = self.native_types[table]
        layout = _type.layout()
        for rec in self._db.select(table, where=where, order=order, **kwargs):
            yield _type(**kwargs_from(rec, layout))

    def read(self,
             table: str,
             where: Optional[str] = None,
             order: Optional[str] = None,
             **kwargs) -> Generator[Record, None, None]:
        """Read (joined) records from database

        Arguments:
            table: Name of table to read from
            where: Search string passed verbatim to
                :meth:`~fsgop.db.database.Database.join`
            order: Sort order. Passed verbatim to
                :meth:`~fsgop.db.database.Database.join`
            **kwargs: Parameters for safe value substitution. Passed verbatim to
                :meth:`~fsgop.db.database.Database.join`

        Yields:
            Native type representation of matching records
        """
        _type = self.native_types[table]
        layout = _type.layout()
        for rec in self._db.join(table,
                                 where=where,
                                 order=order,
                                 depth=2,
                                 **kwargs):
            yield _type(**kwargs_from(rec, layout, None))

    def find(self, records: Iterable[Record]) -> Generator[Record, None, None]:
        """Find records in the database

        Args:
            records: Iterable of (incomplete) records containing information used
                to search the database.

        Yields:
            Matching database record for each record in *records*.
        """
        recs = Sequence(records)
        if not recs:
            return

        _type = recs.element_type
        layout = _type.layout()
        table = self._native_tables[_type]
        rectype = self._db.schema[table].record_type
        col_types = self._db.schema[table].column_types
        _where = " and ".join(f"{k}={self._db.var(k + '_')}"
                              for k in _type.index)

        for rec in recs:
            if rec.uid is not None:
                t = self._db.unique_id(table, rec.uid, rectype=rectype)
            else:
                # convert to native record to make sure we are dealing with
                # correct types for each column
                db_rec = rec.to(rectype, col_types)
                kwargs = {f"{k}_": getattr(db_rec, k) for k in _type.index}
                t = self._db.unique(table,
                                    where=_where,
                                    rectype=rectype,
                                    **kwargs)
            yield _type(**kwargs_from(t, layout, None))

    def add(self,
            table: str,
            recs: Iterable[Record],
            allow: Optional[Callable] = None) -> Generator[Record, None, None]:
        """Add properties to records

        Args:
            table: Name of table containing the properties
            recs: Iterable of records
            allow: Callable accepting a :class:`~fsgop.db.Property` and a
                :class:`~fsgop.db.Record` as arguments which returns a boolean.
                Properties will only be added to records, if the callable returns
                ``True`` for the respective property/record combination. Passing
                ``None`` results in no filter. Defaults to ``None``.

        Yields:
            Modified records
        """
        ptype = self.native_types[table]  # property type
        layout = ptype.layout()
        column = table.split("_")[0]  # "person" or "vehicle"

        # get type of record accepting the properties
        rtable = self._db.schema[table].get_column(column).ref_info[0]
        rtype = self.native_types[rtable]

        for rec in recs:
            dest = {v.uid: v for k, v in rec.select(rtype)}
            where = f"{column} IN ({','.join(map(str, dest.keys()))})"
            for prec in self._db.select(table, where=where):
                _property = ptype(**kwargs_from(prec, layout))
                if allow is None or allow(_property, rec):
                    _property.add_to(dest[_property.rec])
            yield rec

    def insert(self, records: Iterable[Record], force: bool = False) -> tuple:
        """Insert native data records into database

        Args:
            records: Iterable of records to insert
            force: If ``True``, existing records are replaced. Defaults to
              ``False``.
        Returns:
            number of records inserted and number of properties inserted
        """
        recs = Sequence(records)
        if not recs:
            return 0, 0

        table = self._native_tables[recs.element_type]
        rectype = self._db.schema[table].record_type
        col_types = self._db.schema[table].column_types
        nrec = 0
        nins = 0
        n = self._db.count(table)
        for items in chunk(recs, 1024):
            batch = list(items)
            # TODO records could be incomplete if uids are missing
            # -> add a method complete(db) to Record, which completes the record
            # or consider adding constraint, that records have to be complete
            self._db.insert(table,
                            (r.to(rectype, col_types) for r in batch),
                            force=force)
            _n = self._db.count(table)
            logging.debug(f"Inserted batch of {len(batch)} records into table "
                          f"'{table}', kept {_n - n}")
            nins += (_n - n)
            n = _n
            nrec += len(batch)

            missing_uids = [r for r in batch
                            if r.uid is None and r.has_properties]
            for r, _r in zip(missing_uids, self.find(missing_uids)):
                r.uid = _r.uid
            for r in batch:
                self.insert(r.properties, force=force)
        logging.info(f"Inserted {nins}/{nrec} records into table '{table}'")
        return nrec, nins

    def commit(self):
        self._db.commit()

    @classmethod
    def new(cls, path):
        """Create a new repository from scratch"""
        repo = cls(path)
        repo._db.reset()
        return repo

    @classmethod
    def from_startkladde(cls,
                         path: Union[str, Path],
                         db: Union[str, Path]) -> "Repository":
        """Create a new repository from a startkladde database file

        This constructor converts all data from the startkladde database into
        the native data model and then adds it to a new database, which acts as
        storage for the newly created repository.

        Args:
            path: Path to the startkladde database file
            db: Path to the database to create. Will be overwritten if it
                exists.

        Returns:
            New database with native schema
        """
        try:
            with sk.Repository(path) as src:
                dest = cls.new(db)
                # TODO force required if uid is auto-set ?
                dest.insert(src.persons(), force=True)
                dest.insert(src.vehicles(), force=False)
                dest.insert(src.missions(), force=False)
                dest.commit()  # without this, nothing gets written!
                return dest
        except:
            dest.close()
            try:
                Path(db).unlink()
            except FileNotFoundError:
                pass
            raise

    @staticmethod
    def valid_during_mission(prop: Property, mission: Mission) -> bool:
        """Filter function usable in combination with add

        Args:
            prop: Property record
            mission: Mission record

        Returns:
            True if property is valid during the mission.
        """
        return bool(
                   (mission.begin is None or mission.begin >= prop.valid_from)
                   and
                   (mission.end is None or mission.end <= prop.valid_until)
        )
