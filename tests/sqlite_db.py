#!/usr/bin/env python3

import unittest
from fsgop.db import SqliteDatabase, TableInfo
from fsgop.db.startkladde import schema_v3 as sk_schema
from pathlib import Path

TEST_DIR = Path(__file__).parent
DB_PATH = TEST_DIR / "test_sqlite_db.db"


class SqliteDatabaseTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.debug = True
        self.path = str(DB_PATH)

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
        db.create_table(self.table_info("people"))
        schema = db.get_schema()
        actual = schema['people'].as_dict()
        ref = sk_schema['people']

        for col1, col2 in zip(ref['columns'], actual['columns']):
            self.assertDictEqual({k: v for k, v in col1.items() if k != "extra"},
                                 {k: v for k, v in col2.items() if k != "extra"})

        for idx1, idx2 in zip(ref['indices'], actual['indices']):
            ignore = "name" if idx1['is_primary'] else ""
            self.assertDictEqual({k: v for k, v in idx1.items() if k != ignore},
                                 {k: v for k, v in idx2.items() if k != ignore})

    def test_create_database(self):
        db = self.create_db()
        for name in sk_schema.keys():
            db.create_table(self.table_info(name))

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

    @staticmethod
    def table_info(name):
        return TableInfo.from_list(name=name, **sk_schema[name])


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(SqliteDatabaseTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )