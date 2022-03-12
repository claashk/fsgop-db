import logging
from typing import List, NamedTuple, Type, Iterable, Union
from pathlib import Path

from .person import NameAdapter, Person, PersonProperty
from .mission import Mission
from .utils import kwargs_from
from .sqlite_db import SqliteDatabase
from .utils import Sequence, chunk
from .record import Record
from .vehicle import Vehicle
from . import startkladde as sk
from . native_schema import schema_v1

logger = logging.getLogger(__name__)


def import_flights(records: List[NamedTuple]):
    if not records:
        return []

    recs, _type = NameAdapter.apply(records, rectype=type(records[0]))
    layout = Mission.layout(allow=_type._fields)

    for rec in recs:
        yield Mission(**kwargs_from(rec, layout=layout))


class Controller(object):
    """Database controller

    The database controller provides the main API for external applications.
    It uses a database with native schema as data storage and adds methods to
    import and manipulate this data.

    The controller can be used as context manager.

    Args:
        db: database
    """
    native_types = {
        "people": Person,
        "vehicles": Vehicle,
        "person_properties": PersonProperty
    }

    def __init__(self, db) -> None:
        self._db = db

    def __enter__(self) -> "Controller":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._db.__exit__(exc_type, exc_val, exc_tb)

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
    def from_startkladde(cls,
                         path: Union[str, Path],
                         db: Union[str, Path]) -> "Controller":
        """Create a new database from a startkladde instance

        Args:
            path: Path to the startkladde database file
            db: Path to the database to create. Will be overwritten if it
                exists.

        Returns:
            New database with native schema
        """
        with SqliteDatabase(path) as src:
            try:
                _db = SqliteDatabase(db=db, schema=schema_v1)
                _db.reset()
                ctl = cls(db=_db)
                ctl.insert(sk.iter_persons(src), force=True)
                ctl.commit()
            except:
                try:
                    Path(db).unlink()
                except FileNotFoundError:
                    pass
                raise
        return ctl
