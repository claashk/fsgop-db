#!/usr/bin/env python3

import unittest

from fsgop.db import Person
from fsgop.db.controller import Controller


class ControllerTestCase(unittest.TestCase):

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


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(ControllerTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())