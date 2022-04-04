#!/usr/bin/env python3

import unittest
from pathlib import Path
import logging
from io import StringIO

from fsgop.db import Repository, SqliteDatabase, Person
from fsgop.db.vehicle import WINCH
from fsgop.db.startkladde import schema_v3 as sk_schema


DATA_DIR = Path(__file__).parent / "startkladde-dump"
DB_PATH = DATA_DIR.parent / "repo_test.sqlite3"
STARTKLADDE_DB_PATH = DB_PATH.parent / "native_sk_db.sqlite3"

logger = logging.getLogger()


class RepositoryTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.keep_artifacts = False
        with SqliteDatabase.from_dump(DATA_DIR,
                                      schema=sk_schema,
                                      db=str(STARTKLADDE_DB_PATH)) as db:
            pass

    @classmethod
    def tearDownClass(cls) -> None:
        if not cls.keep_artifacts:
            try:
                STARTKLADDE_DB_PATH.unlink()
            except FileNotFoundError:
                pass

    def setUp(self) -> None:
        self.keep_artifacts = True
        self.out = StringIO()
        self.stream_handler = logging.StreamHandler(self.out)
        logger.addHandler(self.stream_handler)
        logger.setLevel(logging.DEBUG)

    def tearDown(self) -> None:
        logger.removeHandler(self.stream_handler)
        if not self.keep_artifacts:
            try:
                DB_PATH.unlink()
            except FileNotFoundError:
                pass

    def create_repo(self):
        with Repository.from_startkladde(STARTKLADDE_DB_PATH, DB_PATH) as repo:
            yield repo

    def test_startkladde_import(self):
        for repo in self.create_repo():
            missions = list(repo.add("vehicle_properties",
                                     repo.read("missions"),
                                     Repository.valid_during_mission))

        self.assertEqual(7, len(missions))
        for m in missions:
            if m.vehicle.category != WINCH:
                self.assertIn(m.vehicle.registration,
                              ("D-1234", "D-2234", "D-EFGH"))

        out = self.out.getvalue()
        #print("Logger Output:", out)
        self.assertEqual(1,
                         out.count("Inserted 5/5 records into table 'people'"))
        self.assertEqual(3,
                         out.count("Inserted 2/2 records into table "
                                   "'person_properties'"))

    def test_replace(self):
        for repo in self.create_repo():
            people = list(repo.select("people", where="first_name='Willi'"))
            self.assertEqual(1, len(people))
            missions = list(repo.read("missions",
                                      where=f"missions.copilot={people[0].uid}"))
            self.assertGreater(len(missions), 0)

            recs = [Person(last_name="Wright", first_name="Willi")]
            replacement = Person(last_name="Wright", first_name="Wilbur")
            repo.replace(recs, replacement)

            new_people = list(repo.select("people", where="first_name='Willi'"))
            self.assertEqual(0, len(new_people))

            new_guy = next(repo.find([replacement])).uid
            for m in repo.find(missions):
                self.assertEqual(new_guy, int(m.copilot))

def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(RepositoryTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
