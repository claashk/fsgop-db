#!/usr/bin/env python3

import unittest
from pathlib import Path

from fsgop.db import SqliteDatabase, Property
from fsgop.db.startkladde import schema_v3, iter_persons


DATA_DIR = Path(__file__).parent / "startkladde-dump"


class StartkladdeTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.keep_artifacts = False
        self.db_path = DATA_DIR.parent / "test_sk_import.sqlite3"
        #self.db_path = ":memory:"

    def tearDown(self) -> None:
        if not self.keep_artifacts:
            try:
                self.db_path.unlink()
            except FileNotFoundError:
                pass

    def test_import(self):
        with SqliteDatabase.from_dump(DATA_DIR,
                                      schema=schema_v3,
                                      db=str(self.db_path)) as db:
            people = list(db.select("people"))
            self.assertEqual(3, len(people))
            self.assertListEqual([1, 2, 3], [p.id for p in people])
            self.assertEqual(len(people), db.count("people"))

            planes = list(db.select("planes"))
            self.assertEqual(3, len(planes))
            self.assertListEqual([1, 2, 10], [p.id for p in planes])

            persons = list(iter_persons(db))
            self.assertEqual(len(people), len(persons))
            for p1, p2 in zip(people, persons):
                self.assertEqual(p1.last_name, p2.last_name)
                self.assertEqual(p1.first_name, p2.first_name)
                self.assertEqual(p1.id, p2.uid)

                med_validity = Property.get_from(p2, "medical").valid_until
                self.assertEqual(p1.medical_validity, str(med_validity))


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(StartkladdeTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())