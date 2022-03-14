#!/usr/bin/env python3

import unittest
from fsgop.db import Person, Vehicle, Mission
from fsgop.db.mission import TWO_SEATED_TRAINING, NORMAL_FLIGHT

from datetime import datetime, timedelta


class MissionTestCase(unittest.TestCase):

    def setUp(self) -> None:
        # Robert Hutchings Goddard (* 5. Oktober 1882 in Worcester, Massachusetts, USA; † 10. August 1945 in Baltimore, Marylan
        self.person1 = Person(first_name="Otto",
                             last_name="Prof. Dr. Lilienthal",
                             birthday="1848-05-23")
        self.person2 = Person(first_name="Robert Hutchings",
                             last_name="Goddard",
                             birthday="1882-10-05")
        self.vehicle = Vehicle(manufacturer="Gulfstream",
                               model="G-550",
                               num_seats=7,
                               registration="D-ADLR",
                               serial_number="G123456")

    def test_construction(self):
        mission = Mission()
        self.assertIsNone(mission.uid)
        self.assertIsNone(mission.pilot)
        self.assertIsNone(mission.copilot)
        self.assertIsNone(mission.passenger1)
        self.assertIsNone(mission.passenger2)
        self.assertIsNone(mission.passenger3)
        self.assertIsNone(mission.passenger4)
        self.assertIsNone(mission.vehicle)
        self.assertEqual(NORMAL_FLIGHT, mission.category)
        self.assertIsNone(mission.begin)
        self.assertIsNone(mission.end)
        self.assertIs(mission.launch, mission)

        m = Mission(pilot=self.person1,
                    copilot=self.person2,
                    vehicle=self.vehicle,
                    begin="2020-12-31 15:00:00",
                    end="2021-01-01 1:20:00")
        self.assertEqual("Otto", m.pilot.first_name)
        self.assertEqual("G123456", m.vehicle.serial_number)
        self.assertEqual(datetime(2021, 1, 1, 1, 20), m.end)

    def test_layout(self):
        person_layout = {
            "first_name": "first_name",
            "last_name": "last_name",
            "title": "title",
            "comments": "comments",
            "birthday": "birthday",
            "birthplace": "birthplace",
            "count": "count",
            "uid": "uid",
            "kind": "kind"
        }

        mission_layout = Mission.layout()

        for x in ["pilot", "copilot", "passenger1"]:
            expected = {k: f"{x}_{v}" for k, v in person_layout.items()}
            self.assertDictEqual(expected, mission_layout[x])

    def test_comparison(self):
        m1 = Mission(pilot=self.person1,
                     copilot=self.person2,
                     vehicle=self.vehicle,
                     begin="2020-12-31 15:00:00",
                     end="2021-01-01 1:20:00")
        m2 = Mission(pilot=self.person2,
                     copilot=self.person1,
                     vehicle=self.vehicle,
                     begin="2020-12-31 15:00:00",
                     end="2020-12-31 16:00:00")
        m3 = Mission(pilot=self.person2,
                     copilot=self.person1,
                     vehicle=self.vehicle,
                     begin="2020-12-31 15:01:00",
                     end="2021-01-01 1:20:00")
        m4 = Mission(uid=55)

        self.assertFalse(m1 < m2)
        self.assertFalse(m2 < m1)
        self.assertTrue(m1 < m3)
        self.assertEqual(m1, m2)
        self.assertNotEqual(m1, m3)
        self.assertEqual(m4, 55)  # key comparison

        with self.assertRaises(TypeError):
            _x = m4 < m3

    def test_pic(self):
        m1 = Mission(pilot=self.person1,
                     copilot=self.person2,
                     vehicle=self.vehicle,
                     begin="2020-12-31 15:00:00",
                     end="2021-01-01 1:20:00")
        self.assertEqual(self.person1, m1.pic())

        m1.category = TWO_SEATED_TRAINING
        self.assertEqual(self.person2, m1.pic())

    def test_crew(self):
        m1 = Mission(pilot=self.person1,
                     copilot=self.person2,
                     vehicle=self.vehicle,
                     begin="2020-12-31 15:00:00",
                     end="2021-01-01 1:20:00")
        self.assertSetEqual({self.person1, self.person2}, m1.crew())

        m2 = Mission(pilot=self.person2,
                     copilot=self.person1,
                     passenger1=Person(55),
                     vehicle=self.vehicle,
                     begin="2020-12-31 15:00:00",
                     end="2020-12-31 16:00:00")
        self.assertSetEqual({self.person1, self.person2, Person(55)}, m2.crew())

    def test_duration(self):
        m1 = Mission(pilot=self.person1,
                     copilot=self.person2,
                     vehicle=self.vehicle,
                     begin="2020-12-31 15:00:00",
                     end="2021-01-01 1:20:00")
        self.assertEqual(timedelta(hours=10, minutes=20), m1.duration())

    def test_almost_equal(self):
        m1 = Mission(pilot=self.person1,
                     copilot=self.person2,
                     vehicle=Vehicle(uid=21),
                     begin="2020-12-31 15:00:00",
                     end="2021-01-01 1:20:00")
        m2 = Mission(pilot=self.person1,
                     vehicle=Vehicle(uid=20),
                     begin="2020-12-31 15:00:00",
                     end="2021-01-01 1:20:00")
        m3 = Mission(pilot=self.person1,
                     vehicle=Vehicle(uid=20),
                     begin=m2.begin + timedelta(hours=24),
                     end=m2.end + timedelta(hours=24))
        m4 = Mission(pilot=self.person2,
                     vehicle=Vehicle(uid=20),
                     begin=m1.begin + timedelta(minutes=1),
                     end=m1.begin + timedelta(minutes=2))

        self.assertTrue(m1.almost_equal(m2))
        self.assertTrue(m2.almost_equal(m1))
        self.assertFalse(m1.almost_equal(m3))
        self.assertFalse(m3.almost_equal(m1))
        self.assertTrue(m1.almost_equal(m4))
        self.assertTrue(m4.almost_equal(m1))
        self.assertTrue(m4.almost_equal(m2))
        self.assertFalse(m4.almost_equal(m3))


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(MissionTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())