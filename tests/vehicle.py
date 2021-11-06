#!/usr/bin/env python3

import unittest
from fsgop.db import Vehicle
from fsgop.db.utils import to

from datetime import date
from collections import namedtuple


class VehicleTestCase(unittest.TestCase):

    def test_construction(self):
        v = Vehicle()
        self.assertEqual("", v.manufacturer)
        self.assertEqual("", v.model)
        self.assertEqual("", v.serial)
        self.assertEqual(1, v.num_seats)
        self.assertIsNone(v.uid)
        self.assertIsNone(v.category)

        v1 = Vehicle(manufacturer="Grob",
                     model="G 103",
                     serial="G 103 123456",
                     num_seats="2")
        self.assertEqual("Grob", v1.manufacturer)
        self.assertEqual("G 103", v1.model)
        self.assertEqual("G 103 123456", v1.serial)
        self.assertEqual(2, v1.num_seats)

    def test_fields(self):
        self.assertSetEqual({"uid",
                             "manufacturer",
                             "model",
                             "comments",
                             "num_seats",
                             "serial",
                             "category"},
                            Vehicle.fields())

        self.assertSetEqual(set(), Vehicle.nested_records())

    def test_from_dict(self):
        d = {"manufacturer": "Grob",
             "model": "G 103B",
             "club": "FSG Schwaben",
             "category": 2,
             "serial": "321"}
        v = Vehicle.from_dict(d)
        self.assertIsInstance(v, Vehicle)
        self.assertEqual("Grob", v.manufacturer)
        self.assertEqual("", v.model)
        self.assertEqual("321", v.serial)
        self.assertIsNone(v.category)

        common_fields = Vehicle.fields_in(d.keys())
        v = Vehicle.from_dict(d, common_fields)
        self.assertIsInstance(v, Vehicle)
        self.assertEqual("Grob", v.manufacturer)
        self.assertEqual("G 103B", v.model)
        self.assertEqual(2, v.category)
        self.assertEqual("321", v.serial)

    def test_from_namedtuple(self):
        TupleType = namedtuple("TupleType",
                               ["manufacturer", "model", "club", "serial"])
        t = TupleType("Grob", "G103B", "A club", "321")
        recognised_fields = Vehicle.fields_in(TupleType._fields)
        v = Vehicle.from_obj(t, ["manufacturer", "model"])
        self.assertIsInstance(v, Vehicle)
        self.assertEqual("Grob", v.manufacturer)
        self.assertEqual("G103B", v.model)
        self.assertEqual("", v.serial)

        v = Vehicle.from_obj(t, recognised_fields)
        self.assertIsInstance(v, Vehicle)
        self.assertEqual("Grob", v.manufacturer)
        self.assertEqual("G103B", v.model)
        self.assertEqual("321", v.serial)

    def test_comparison(self):
        v1 = Vehicle(manufacturer="Grob", model="G103B", serial="AB321")
        v2 = Vehicle(manufacturer="Grob", model="G103C", serial="AB321")
        v3 = Vehicle(manufacturer="Grob", model="G103B", serial="CD321")
        self.assertFalse(v1 < v2)
        self.assertFalse(v2 < v1)
        self.assertTrue(v1 < v3)
        self.assertEqual(v1, v2)
        self.assertNotEqual(v1, v3)

    def test_uid_construction(self):
        v = to(Vehicle, 123)
        self.assertEqual(123, v.uid)
        self.assertEqual(123, int(v))


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(VehicleTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )