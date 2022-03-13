#!/usr/bin/env python3

import unittest
from fsgop.db import SqliteDatabase, to_schema
from fsgop.db.startkladde import schema_v3
from fsgop.db.native_schema import schema_v1 as native_schema
from pathlib import Path

TEST_DIR = Path(__file__).parent
DB_PATH = TEST_DIR / "test_sqlite_db.db"


class SqliteDatabaseTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.debug = False
        self.path = str(DB_PATH)
        self.schema = to_schema(schema_v3)

    def tearDown(self) -> None:
        if not self.debug and DB_PATH.exists():
            DB_PATH.unlink()

    def create_db(self):
        if DB_PATH.exists():
            DB_PATH.unlink()
        return SqliteDatabase(self.path)

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
        property_type = db.schema["person_properties"].record_type

        p1 = person_type(uid=None,
                         last_name="Hopper",
                         first_name="Harry",
                         title=None,
                         birthday=None,
                         birthplace=None,
                         count=1,
                         kind=1,
                         comments=None)
        db.insert("people", [p1], force=True)
        self.assertEqual(1, db.count("people"))
        people = list(db.select("people"))


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(SqliteDatabaseTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())