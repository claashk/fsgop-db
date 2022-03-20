import logging
from typing import Type, Iterable, Union
from pathlib import Path

from .person import Person, PersonProperty
from .vehicle import Vehicle, VehicleProperty
from .mission import Mission
from .sqlite_db import SqliteDatabase
from .utils import Sequence, chunk
from .record import Record
from . import startkladde as sk
from . native_schema import schema_v1

logger = logging.getLogger(__name__)


class Repository(object):
    """Repository implementation

    The repository is an intermediate layer between database and application (
    controllers, views). It wraps a database with a given schema and allows I/O
    operations using the native datamodel (Person, Vehicle, Mission). This
    is the main distinction to the database, which works data model agnostic and
    uses only tuples and the native schema for I/O operations.

    The repository can be used as context manager.

    Args:
        db: database
    """
    native_types = {
        "people": Person,
        "vehicles": Vehicle,
        "person_properties": PersonProperty,
        "vehicle_properties": VehicleProperty,
        "missions": Mission
    }

    def __init__(self, db: str) -> None:
        self._db = None
        if db:
            self.open(db)

    def __enter__(self) -> "Repository":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def open(self, db: str, **kwargs) -> None:
        self.close()
        self._db = SqliteDatabase(db=db, schema=schema_v1, **kwargs)

    def close(self):
        if self._db is not None:
            self._db.disconnect()
            self._db = None

    def insert(self, recs: Iterable[Record], force: bool = False) -> tuple:
        """Insert native data records into database

        Args:
            recs: Iterable of records to insert
            force: If ``True``, existing records are replaced. Defaults to
              ``False``.
        Returns:
            number of records inserted and number of properties inserted
        """
        _recs = Sequence(recs)
        if not _recs:
            return 0, 0

        table = self._get_table(_recs.element_type)
        rectype = self._db.schema[table].record_type
        col_types = self._db.schema[table].column_types
        nrec = 0
        nins = 0
        n = self._db.count(table)
        for items in chunk(_recs, 1024):
            batch = list(items)
            # TODO records could be incomplete if uids are missing
            # -> add a method complete(db) to Record, which completes the record
            self._db.insert(table,
                            (r.to(rectype, col_types) for r in batch),
                            force=force)
            _n = self._db.count(table)
            logging.debug(f"Inserted batch of {len(batch)} records into table "
                          f"'{table}', kept {_n - n}")
            nins += (_n - n)
            n = _n
            nrec += len(batch)
            for r in batch:
                properties = list(r.properties)
                if properties:
                    if r.uid is None:
                        r.uid = r.search_in(self._db, table).uid
                    self.insert(properties, force=force)
        logging.info(f"Inserted {nins}/{nrec} records into table '{table}'")
        return nrec, nins

    def commit(self):
        self._db.commit()

    def _get_table(self, t: Type) -> str:
        """Get table for a given native datatype

        Args:
            t: Native data type

        Return:
            Name of table storing elements of type t

        Raises:
            KeyError: If type is not recognised
        """
        for k, v in self.native_types.items():
            if t is v:
                return k
        raise KeyError(f"No native type found for {t}")

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
        """Create a new repository from a startkladde instance

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
            with sk.Repository(path) as src, cls.new(db) as dest:
                dest.insert(src.persons(), force=True)
                dest.insert(src.vehicles(), force=True)
                dest.insert(src.missions(), force=True)
                dest.commit()  # without this, nothing gets written!
                return dest
        except:
            try:
                Path(db).unlink()
            except FileNotFoundError:
                pass
            raise
