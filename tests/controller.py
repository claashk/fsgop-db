#!/usr/bin/env python3
import unittest
from pathlib import Path
from datetime import datetime

from fsgop.db import Controller, Vehicle
from fsgop.db import Person, Repository, SqliteDatabase
from fsgop.db.startkladde import schema_v3 as sk_schema


class ControllerTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.keep_artifacts = False
        cls.data_src = Path(__file__).parent / "test-data" / "startkladde-dump"
        cls.db_path = cls.data_src.parent.parent / "artifacts" / "controller_test.sqlite3"
        cls.tmp_db_path = cls.db_path.parent / "native_sk_db.sqlite3"

        with SqliteDatabase.from_dump(cls.data_src,
                                      schema=sk_schema,
                                      db=str(cls.tmp_db_path)) as db:
            pass

    @classmethod
    def tearDownClass(cls) -> None:
        if not cls.keep_artifacts:
            try:
                cls.tmp_db_path.unlink()
            except FileNotFoundError:
                pass

    def setUp(self) -> None:
        self.person1 = Person(first_name="Otto",
                              last_name="Prof. Dr. Lilienthal",
                              birthday="1848-05-23")
        self.person2 = Person(first_name="Robert Hutchings",
                              last_name="Goddard",
                              birthday="1882-10-05")
        self.person3 = Person(first_name="Robert H.",
                              last_name="Goddard",
                              birthday="1882-10-05")

    def tearDown(self) -> None:
        if not self.keep_artifacts:
            try:
                self.db_path.unlink()
            except FileNotFoundError:
                pass

    def create_ctrl(self):
        with Repository.from_startkladde(self.tmp_db_path, self.db_path) as repo:
            yield Controller(repo)

    def test_match(self):
        l1 = [self.person1, self.person3, self.person2]
        l2 = [self.person1, self.person2]

        match = Controller.match(l1, l2, 1.)
        self.assertListEqual([self.person3],
                             [m.rec1 for m in match if m.rec2 is None])
        self.assertListEqual([], [m.rec2 for m in match if m.rec1 is None])
        self.assertEqual(2, len([m for m in match if None not in m]))
        for p in (self.person1, self.person2):
            self.assertListEqual([p], [m.rec1 for m in match if m.rec2 is p])

        l1 = [self.person1, self.person3]
        l2 = [self.person1, self.person2]
        match = Controller.match(l1, l2, 1.)
        self.assertListEqual([self.person3],
                             [m.rec1 for m in match if m.rec2 is None])
        self.assertListEqual([self.person2],
                             [m.rec2 for m in match if m.rec1 is None])
        self.assertEqual(1, len([m for m in match if None not in m]))
        self.assertListEqual([self.person1],
                             [m.rec1 for m in match if m.rec2 is self.person1])

        l1 = [self.person1, self.person3]
        l2 = [self.person1, self.person2]
        match = Controller.match(l1, l2, 0.5)
        self.assertListEqual([], [m.rec1 for m in match if m.rec2 is None])
        self.assertListEqual([], [m.rec2 for m in match if m.rec1 is None])
        self.assertEqual(2, len([m for m in match if None not in m]))

        self.assertListEqual([self.person1],
                             [m.rec1 for m in match if m.rec2 is self.person1])
        self.assertListEqual([self.person3],
                             [m.rec1 for m in match if m.rec2 is self.person2])

    def test_flights_of(self):
        for ctrl in self.create_ctrl():
            wilbur = Person(last_name="Wright", first_name="Wilbur")

            count = 0
            for flight in ctrl.missions_of(wilbur):
                count += 1
                self.assertEqual("Wilbur", flight.pilot.first_name)
            self.assertEqual(2, count)

            otto = Person(1)
            count = 0
            for flight in ctrl.missions_of(otto, since=datetime(2020, 4, 10)):
                count += 1
                self.assertIn(otto.uid, [p.uid for p in flight.crew])
            self.assertEqual(2, count)

            count = 0
            for flight in ctrl.missions_of(otto):
                count += 1
                self.assertIn(otto.uid, [p.uid for p in flight.crew])
            self.assertEqual(4, count)

    def test_missions_like(self):
        for ctrl in self.create_ctrl():
            missions = list(ctrl._repo.read("missions"))
            for m in missions:
                similar = list(ctrl.missions_like(m))
                self.assertEqual(1, len(similar))
                self.assertEqual(m.uid, similar[0].uid)
            m = missions[0]
            m.vehicle.uid = 999
            self.assertEqual(1, len(list(ctrl.missions_like(m))))
            m.pilot.uid = 999
            self.assertEqual(1, len(list(ctrl.missions_like(m))))
            m.copilot.uid = 998
            self.assertEqual(0, len(list(ctrl.missions_like(m))))

    def test_vehicles(self):
        for ctrl in self.create_ctrl():
            vehicles = list(ctrl.vehicles())
            where = f"category={Vehicle.categories['glider']}"
            gliders = list(ctrl.vehicles(where=where))

        self.assertEqual(6, len(vehicles))
        self.assertEqual(2, len(gliders))
        self.assertTrue(all(isinstance(v, Vehicle) for v in vehicles))
        self.assertSetEqual({"D-1234", "D-2234"},
                            {v.registration for v in gliders})


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(ControllerTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())