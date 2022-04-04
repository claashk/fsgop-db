#!/usr/bin/env python3

import unittest
from pathlib import Path
from datetime import timedelta
import logging
from io import StringIO

from fsgop.db import SqliteDatabase, Property
import fsgop.db.startkladde as sk


DATA_DIR = Path(__file__).parent / "startkladde-dump"
logger = logging.getLogger()


class StartkladdeTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.keep_artifacts = False
        self.db_path = DATA_DIR.parent / "test_sk_import.sqlite3"
        self.out = StringIO()
        self.stream_handler = logging.StreamHandler(self.out)
        for h in logger.handlers:
            logger.removeHandler(h)
        logger.addHandler(self.stream_handler)
        logger.setLevel(logging.INFO)

        #self.db_path = ":memory:"

    def tearDown(self) -> None:
        logger.removeHandler(self.stream_handler)
        if not self.keep_artifacts:
            try:
                self.db_path.unlink()
            except FileNotFoundError:
                pass

    def test_import(self):
        with SqliteDatabase.from_dump(DATA_DIR,
                                      schema=sk.schema_v3,
                                      db=str(self.db_path)) as db:
            people = list(db.select("people"))
            self.assertEqual(5, len(people))
            self.assertListEqual([1, 2, 3, 4, 79], [p.id for p in people])
            self.assertEqual(len(people), db.count("people"))

            planes = list(db.select("planes"))
            self.assertEqual(3, len(planes))
            self.assertListEqual([1, 2, 10], [p.id for p in planes])

            flights = list(db.select("flights"))
            self.assertEqual(4, len(flights))

            joined_flights = list(db.join("flights"))
            self.assertEqual(len(flights), len(joined_flights))

            _people = {p.id: p for p in people}
            for i in range(len(flights)):
                self.assertEqual(_people[flights[i].pilot_id].first_name,
                                 joined_flights[i].pilot_id_first_name)

                self.assertEqual(_people[flights[i].pilot_id].last_name,
                                 joined_flights[i].pilot_id_last_name)

                if flights[i].copilot_id is None:
                    self.assertIsNone(joined_flights[i].copilot_id_first_name)
                    self.assertIsNone(joined_flights[i].copilot_id_last_name)
                else:
                    self.assertEqual(_people[flights[i].copilot_id].first_name,
                                     joined_flights[i].copilot_id_first_name)

                    self.assertEqual(_people[flights[i].copilot_id].last_name,
                                     joined_flights[i].copilot_id_last_name)

            repo = sk.Repository(db=db)
            persons = list(repo.persons())
            self.assertEqual(len(people), len(persons))
            for p1, p2 in zip(people, persons):
                self.assertEqual(p1.last_name, p2.last_name)
                self.assertEqual(p1.first_name, p2.first_name)
                self.assertEqual(p1.id, p2.uid)
                if p1.last_name != "Gast":
                    med_validity = Property.get_from(p2, "medical").valid_until
                    med_validity = (med_validity - timedelta(hours=24)).date()
                    self.assertEqual(p1.medical_validity, str(med_validity))


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(StartkladdeTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())