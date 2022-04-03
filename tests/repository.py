#!/usr/bin/env python3

import unittest
from pathlib import Path
import logging
from io import StringIO

from fsgop.db import Repository, SqliteDatabase
from fsgop.db.vehicle import WINCH
from fsgop.db.startkladde import schema_v3 as sk_schema


DATA_DIR = Path(__file__).parent / "startkladde-dump"
logger = logging.getLogger()


class RepositoryTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.keep_artifacts = True
        self.db_path = DATA_DIR.parent / "repo_test.sqlite3"
        self.sk_path = self.db_path.parent / "native_sk_db.sqlite3"
        self.out = StringIO()
        self.stream_handler = logging.StreamHandler(self.out)
        logger.addHandler(self.stream_handler)
        logger.setLevel(logging.DEBUG)

    def tearDown(self) -> None:
        logger.removeHandler(self.stream_handler)
        if not self.keep_artifacts:
            for p in (self.db_path, self.sk_path):
                try:
                    p.unlink()
                except FileNotFoundError:
                    pass

    def test_startkladde_import(self):
        with SqliteDatabase.from_dump(DATA_DIR,
                                      schema=sk_schema,
                                      db=str(self.sk_path)) as db:
            pass

        with Repository.from_startkladde(self.sk_path, self.db_path) as repo:
            missions = list(repo.add("vehicle_properties",
                                     repo.read("missions"),
                                     Repository.valid_during_mission))

        self.assertEqual(6, len(missions))
        for m in missions:
            if m.vehicle.category != WINCH:
                self.assertIn(m.vehicle.registration,
                              ("D-1234", "D-2234", "D-EFGH"))

        out = self.out.getvalue()
        #print("Logger Output:", out)
        self.assertEqual(1,
                         out.count("Inserted 4/4 records into table 'people'"))
        self.assertEqual(2,
                         out.count("Inserted 2/2 records into table "
                                   "'person_properties'"))


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(RepositoryTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
