#!/usr/bin/env python3
import sqlite3
import unittest
import logging
from io import StringIO
from fsgop.db import SqliteDatabase, to_schema
from fsgop.db.startkladde import schema_v3
from fsgop.db.native_schema import schema_v1 as native_schema
from pathlib import Path


TEST_DIR = Path(__file__).parent
DB_PATH = TEST_DIR / "artifacts" / "test_sqlite_db.db"
DATA_PATH = TEST_DIR / "test-data" / "startkladde-dump"
logger = logging.getLogger()


class SqliteDatabaseTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.debug = False
        self.path = str(DB_PATH)
        self.schema = to_schema(schema_v3)
        self.out = StringIO()
        self.stream_handler = logging.StreamHandler(self.out)
        for h in logger.handlers:
            logger.removeHandler(h)
        logger.addHandler(self.stream_handler)
        logger.setLevel(logging.WARN)

    def tearDown(self) -> None:
        logger.removeHandler(self.stream_handler)
        if not self.debug and DB_PATH.exists():
            DB_PATH.unlink()

    def create_db(self):
        if DB_PATH.exists():
            DB_PATH.unlink()
        return SqliteDatabase(self.path)

    def load_dump(self):
        return SqliteDatabase.from_dump(DATA_PATH,
                                        schema=schema_v3,
                                        db=str(DB_PATH))

    def test_creation(self):
        db = self.create_db()
        self.assertTrue(DB_PATH.exists())

    def test_create_table(self):
        self.maxDiff = None
        db = self.create_db()
        db.create_table(self.schema["people"])
        schema = db.get_schema()
        actual = schema['people'].as_dict()
        ref = self.schema['people'].as_dict()

        for col1, col2 in zip(ref['columns'], actual['columns']):
            self.assertDictEqual({k: v for k, v in col1.items() if k != "extra"},
                                 {k: v for k, v in col2.items() if k != "extra"})

        for idx1, idx2 in zip(ref['indices'], actual['indices']):
            ignore = "name" if idx1['is_primary'] else ""
            self.assertDictEqual({k: v for k, v in idx1.items() if k != ignore},
                                 {k: v for k, v in idx2.items() if k != ignore})

    def test_create_database(self):
        db = self.create_db()
        db.reset(self.schema)
        schema = db.get_schema()
        self.assertSetEqual({"flights",
                             "launch_methods",
                             "people",
                             "planes",
                             "schema_migrations"},
                            set(schema.keys()))
        self.assertEqual("plane_id",
                         schema["flights"].get_column("plane_id").name)
        self.assertEqual("planes(id)",
                         schema["flights"].get_column("plane_id").references)

    def test_native_schema(self):
        db = self.create_db()
        db.reset(native_schema)
        person_type = db.schema["people"].record_type
        p1 = person_type(uid=None,
                         last_name="Hopper",
                         first_name="Harry",
                         title=None,
                         birthday=None,
                         birthplace=None,
                         count=1,
                         kind=1,
                         comments=None)

        self.assertEqual(-1, db.max_id("people"))
        db.insert("people", [p1], force=True)
        self.assertEqual(1, db.count("people"))
        self.assertEqual(1, db.max_id("people"))

    def test_select(self):
        db = self.load_dump()
        people = list(db.select("people", where="first_name='Willi'"))
        self.assertEqual(1, len(people))
        self.assertEqual("Willi", people[0].first_name)
        self.assertEqual("Wright", people[0].last_name)

        people = list(db.select("people", where="last_name='Wright'"))
        self.assertEqual(3, len(people))
        self.assertSetEqual({"Wilbur", "Willi", "Orville"},
                            {p.first_name for p in people})

    def test_delete(self):
        db = self.load_dump()
        flights = list(db.select("flights", order="id"))
        self.assertEqual(4, len(flights))
        self.assertListEqual([16, 21, 31, 32], [f.id for f in flights])

        self.assertEqual(5, db.count("people"))
        with self.assertRaises(sqlite3.IntegrityError) as ctx:
            db.delete("people", where="id='4'")
        self.assertIn("foreign key constraint failed",
                      str(ctx.exception).lower())
        self.assertEqual(5, db.count("people"))

        db.delete("flights", where="copilot_id=4")
        new_flights = list(db.select("flights", order="id"))
        self.assertEqual(3, len(new_flights))
        self.assertListEqual([16, 21, 31], [f.id for f in new_flights])

        db.delete("people", where="id=4")
        self.assertEqual(4, db.count("people"))

    def test_replace(self):
        db = self.load_dump()
        flights = list(db.select("flights", where="copilot_id=4"))
        self.assertEqual(1, len(flights))
        self.assertEqual(1, db.count("people", where="id=4"))

        db.replace("people", where="first_name='Willi'", by=2)
        flight = db.unique("flights", where=f"id={flights[0].id}")
        self.assertEqual(2, flight.copilot_id)
        for fld in type(flight)._fields:
            if fld != "copilot_id":
                self.assertEqual(getattr(flights[0], fld), getattr(flight, fld))
        self.assertEqual(0, db.count("people", where="id=4"))


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(SqliteDatabaseTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())