#!/usr/bin/env python3

import unittest
from pathlib import Path
import logging
from io import StringIO
from datetime import date, datetime

from fsgop.db import Repository, SqliteDatabase, Person, Vehicle, Mission
from fsgop.db import NameAdapter, DateTimeAdapter
from fsgop.db.vehicle import WINCH
from fsgop.db.mission import WINCH_OPERATION
from fsgop.db.startkladde import schema_v3 as sk_schema


DATA_DIR = Path(__file__).parent / "test-data" / "startkladde-dump"
CSV_PATH = DATA_DIR.parent
DB_PATH = DATA_DIR.parent.parent / "artifacts" / "repo_test.sqlite3"
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

    def test_update(self):
        for repo in self.create_repo():
            people = list(repo.select("people", order="uid"))
            self.assertEqual(5, len(people))
            self.assertEqual("Otto K.", people[0].first_name)
            people[0].first_name = "Otto"
            self.assertEqual("Willi", people[3].first_name)
            people[3].first_name = "Martin"
            self.assertEqual("N.n.", people[4].first_name)
            people[4].first_name = None
            repo.update(people, fields={"first_name"})

            _people = list(repo.select("people", order="uid"))
            self.assertListEqual(["Otto", "Martin", ""],
                                 [_people[i].first_name for i in [0, 3, 4]])

    def test_load_file(self):
        people = []
        properties = []
        _parser = lambda s: datetime.strptime(s, "%Y-%m-%d") if s else None
        parsers = {"valid_from": _parser, "valid_until": _parser}
        adapters = [NameAdapter()]

        for repo in self.create_repo():
            people.extend(repo.read_file(CSV_PATH / "people.csv",
                                         adapters=adapters))
            properties.extend(repo.read_file(CSV_PATH / "person_properties.csv",
                                             adapters=adapters,
                                             parsers=parsers))

        self.assertEqual(4, len(people))
        self.assertEqual("Bingo", people[0].last_name)
        self.assertEqual("Benny", people[0].first_name)
        self.assertEqual("Kirk", people[1].last_name)
        self.assertEqual(date(1981, 1, 2), people[2].birthday)
        self.assertIsNone(people[3].birthday)

        self.assertEqual(3, len(properties))
        self.assertEqual("Benny", properties[0].person.first_name)
        self.assertEqual("Bingo", properties[0].person.last_name)
        self.assertEqual("licence", properties[0].kind)
        self.assertEqual("FI-SPL-123456", properties[0].value)
        self.assertEqual("licence", properties[1].kind)
        self.assertEqual("FI-SEP-4321", properties[1].value)

    def test_load_flights(self):
        _time_parser = lambda s: datetime.strptime(s, "%d.%m.%Y %H:%M") if s else None

        parsers = {
            "begin": _time_parser,
            "end": _time_parser
        }
        translate = {
            "Datum": "begin_date",
            "Kennzeichen": "vehicle_registration",
            "Pilot": "pilot_name",
            "Begleiter": "copilot_name",
            "Startort": "origin",
            "Zielort": "destination",
            "Start": "begin_time",
            "Landung": "end_time",
            "Startart": "launch"
        }
        flights = []
        adapters = [NameAdapter(), DateTimeAdapter()]
        for repo in self.create_repo():
            flights.extend(repo.read_file(CSV_PATH / "flights.csv",
                                          headings=["Datum", "Muster"],
                                          table="missions",
                                          translate=translate,
                                          parsers=parsers,
                                          adapters=adapters,
                                          delimiter=";"))

        self.assertEqual(3, len(flights))
        for flight in flights:
            self.assertIsInstance(flight, Mission)
            self.assertIsInstance(flight.pilot, Person)
            self.assertIsInstance(flight.vehicle, Vehicle)
            self.assertIsInstance(flight.launch, Mission)
            self.assertEqual("Wilbur", flight.pilot.first_name)
            self.assertEqual("Wright", flight.pilot.last_name)
        self.assertEqual("Lilienthal", flights[0].copilot.last_name)
        self.assertEqual("Otto", flights[0].copilot.first_name)

        self.assertEqual("D-1234", flights[0].vehicle.registration)
        self.assertEqual("D-2234", flights[1].vehicle.registration)
        self.assertEqual("D-EFGH", flights[2].vehicle.registration)

        self.assertEqual(WINCH_OPERATION, flights[0].launch.category)
        self.assertEqual(WINCH_OPERATION, flights[1].launch.category)
        self.assertIs(flights[2], flights[2].launch)

        self.assertEqual(datetime(2019, 7, 30, 9, 23, 0), flights[0].begin)
        self.assertEqual(datetime(2019, 7, 31, 14, 40, 0), flights[1].begin)
        self.assertEqual(datetime(2019, 8, 2, 11, 52, 0), flights[2].begin)

        self.assertEqual(datetime(2019, 7, 30, 9, 34, 0), flights[0].end)
        self.assertEqual(datetime(2019, 7, 31, 15, 8, 0), flights[1].end)
        self.assertEqual(datetime(2019, 8, 2, 16, 34, 0), flights[2].end)


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(RepositoryTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
