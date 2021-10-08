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
        return SqliteDatabase(self.path)

    def test_creation(self):
        db = self.create_db()
        self.assertTrue(DB_PATH.exists())

    def test_create_table(self):
        self.maxDiff = None
        db = self.create_db()
        db.create_table(self.table_info("people"))
        schema = db.get_schema()
        self.assertDictEqual(sk_schema['people'], schema['people'].as_dict())

    @staticmethod
    def table_info(name):
        return TableInfo.from_list(name=name, **sk_schema[name])


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(SqliteDatabaseTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )