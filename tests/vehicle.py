#!/usr/bin/env python3

import unittest
from fsgop.db import Vehicle, VehicleProperty
from fsgop.db.utils import to


class VehicleTestCase(unittest.TestCase):

    def test_construction(self):
        v = Vehicle()
        self.assertIsNone(v.manufacturer)
        self.assertIsNone(v.model)
        self.assertIsNone(v.serial_number)
        self.assertEqual(1, v.num_seats)
        self.assertIsNone(v.uid)
        self.assertIsNone(v.category)

        v1 = Vehicle(manufacturer="Grob", model="G 103",
                     serial_number="G 103 123456", num_seats="2")
        self.assertEqual("Grob", v1.manufacturer)
        self.assertEqual("G 103", v1.model)
        self.assertEqual("G 103 123456", v1.serial_number)
        self.assertEqual(2, v1.num_seats)

    def test_layout(self):
        layout = {
            "uid": "uid",
            "manufacturer": "manufacturer",
            "model": "model",
            "comments": "comments",
            "num_seats": "num_seats",
            "registration": "registration",
            "serial_number": "serial_number",
            "category": "category"
        }
        self.assertDictEqual(layout, Vehicle.layout())

    def test_comparison(self):
        v1 = Vehicle(manufacturer="Grob", model="G103B", serial_number="AB321")
        v2 = Vehicle(manufacturer="Grob", model="G103C", serial_number="AB321")
        v3 = Vehicle(manufacturer="Grob", model="G103B", serial_number="CD321")
        self.assertFalse(v1 < v2)
        self.assertFalse(v2 < v1)
        self.assertTrue(v1 < v3)
        self.assertEqual(v1, v2)
        self.assertNotEqual(v1, v3)

    def test_uid_construction(self):
        v = to(Vehicle, 123)
        self.assertEqual(123, v.uid)
        self.assertEqual(123, int(v))

    def test_property_layout(self):
        layout = {"uid": "uid",
                  "vehicle": Vehicle.layout(prefix="vehicle_"),
                  "valid_from": "valid_from",
                  "valid_until": "valid_until",
                  "kind": "kind",
                  "value": "value"}

        self.assertDictEqual(layout, VehicleProperty.layout())

    def test_is_glider(self):
        v = Vehicle(manufacturer="Grob",
                    category="glider",
                    model="G103B",
                    serial_number="AB321")
        self.assertTrue(v.is_glider())

        v = Vehicle(manufacturer="Schempp-Hirth",
                    category="motor glider",
                    model="Arcus T",
                    serial_number="AB321")
        self.assertTrue(v.is_glider())

        v = Vehicle(manufacturer="Diamond",
                    category="touring motor glider",
                    model="Super Dimona",
                    serial_number="AB321")
        self.assertFalse(v.is_glider())

        v = Vehicle(manufacturer="Diamond",
                    category="single engine piston",
                    model="DA20",
                    serial_number="AB321")
        self.assertFalse(v.is_glider())


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(VehicleTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )