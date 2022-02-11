#!/usr/bin/env python3

import unittest
from fsgop.db.utils import ASCII, iter_attrs, copy_attrs, all_attrs_equal
from fsgop.db.utils import get_value, set_value, chunk


class MyClass(object):
    def __init__(self, first="first", second=2, third=3.):
        self.first = first
        self.second = second
        self.third = third

    @property
    def fourth(self):
        return 42


class EmptyClass(object):
    pass


class UtilsTestCase(unittest.TestCase):
    def test_ascii_conversion(self):
        german = [
            "älter",
            "Die Älteren",
            "über",
            "Über",
            "örtlich",
            "Das Örtliche",
            "O'Neal"
        ]
        ascii = [
            "aelter",
            "Die Aelteren",
            "ueber",
            "Ueber",
            "oertlich",
            "Das Oertliche",
            "ONeal"
        ]

        for asc, ger in zip(ascii, german):
            self.assertEqual(asc, ger.translate(ASCII))

    def test_iter_attrs(self):
        c = MyClass()
        names, values = zip(*iter_attrs(c))
        self.assertSequenceEqual(names, ["first", "second", "third"])
        self.assertSequenceEqual(values, ["first", 2, 3.])

        names, values = zip(*iter_attrs(c, ignore=["second"]))
        self.assertSequenceEqual(names, ["first", "third"])
        self.assertSequenceEqual(values, ["first", 3.])

    def test_copy(self):
        c1 = MyClass()
        c2 = MyClass(first="third", second=3, third=None)
        copy_attrs(c1, c2)
        names, values = zip(*iter_attrs(c2))
        self.assertSequenceEqual(names, ["first", "second", "third"])
        self.assertSequenceEqual(values, ["first", 2, 3.])

        c3 = EmptyClass()
        copy_attrs(c1, c3, ignore=["second"])
        names, values = zip(*iter_attrs(c3))
        self.assertSequenceEqual(names, ["first", "third"])
        self.assertSequenceEqual(values, ["first", 3.])

    def test_equal_attrs(self):
        c1 = MyClass()
        self.assertTrue(all_attrs_equal(c1, c1))

        c2 = MyClass(first="third", second=3, third=None)
        self.assertFalse(all_attrs_equal(c1, c2))
        c2.first = c1.first
        c2.second = c1.second
        self.assertTrue(all_attrs_equal(c1, c2, ignore=["third"]))
        self.assertFalse(all_attrs_equal(c1, c2))

        c3 = EmptyClass()
        self.assertFalse(all_attrs_equal(c1, c3))
        self.assertFalse(all_attrs_equal(c2, c3))

    def test_get_value(self):
        comment = "email='harry.hopper@home.net'"
        self.assertEqual("harry.hopper@home.net", get_value(comment, "email"))

    def test_set_value(self):
        self.assertEqual("key='a value'", set_value("key", "a value"))
        self.assertEqual("key1='a value'; key2='another value'",
                         set_value("key2", "another value", "key1='a value'"))
        self.assertEqual("key='yes, another value'",
                         set_value("key", "yes, another value", "key='a value'"))

    def test_chunk(self):
        for i, packet in enumerate(chunk(range(50), 10)):
            self.assertListEqual(list(range(10*i, 10*(i+1))), list(packet))

        for i, packet in enumerate(chunk(range(55), 10)):
            if i < 5:
                self.assertListEqual(list(range(10 * i, 10 * (i + 1))),
                                     list(packet))
            else:
                self.assertListEqual(list(range(50, 55)), list(packet))

        for i, packet in enumerate(chunk(range(55), 1)):
            self.assertEqual(i, packet)

        for i, packet in enumerate(chunk(range(53), -1)):
            self.assertEqual(i, 0)
            self.assertListEqual(list(range(53)), list(packet))


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(UtilsTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )