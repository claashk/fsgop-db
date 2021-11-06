#!/usr/bin/env python3

import unittest
from fsgop.db import Person
import fsgop.db.person
from fsgop.db.person import split_title, split_count

from datetime import date
from collections import namedtuple


class PersonTestCase(unittest.TestCase):
    def test_split_title(self):
        names = [
            ("Prof. Dr. ", "Franz H. Meyer"),
            ("Prof. Dr. rer. nat.", "Thomas H. Edison"),
            ("M.Sc. ", "Willy de Vries"),
            ("", "John Doe")
        ]
        for t, n in names:
            name, title = split_title(f"{t}{n}")
            self.assertEqual(n, name)
            if t:
                self.assertEqual(t.strip(), title)
            else:
                self.assertIsNone(title)

    def test_split_count(self):
        names = [
            ("Franz Meyer", 3),
            ("Paul", 4)
        ]
        for n, i in names:
            name, count = split_count(f"{n} ({i})")
            self.assertEqual(n, name)
            self.assertEqual(i, count)

    def test_construction(self):
        person = Person()
        self.assertEqual("", person.last_name)
        self.assertEqual("", person.first_name)
        self.assertEqual("", person.title)
        self.assertEqual("", person.comments)
        self.assertIsNone(person.uid)
        self.assertEqual(1, person.count)

        p = Person(first_name="Otto",
                   last_name="Prof. Dr. Lilienthal",
                   birthday="1848-05-23")
        self.assertEqual("Otto", p.first_name)
        self.assertEqual("Lilienthal", p.last_name)
        self.assertEqual("Prof. Dr.", p.title)
        self.assertEqual(date(1848, 5, 23), p.birthday)

        p = Person(first_name="Otto",
                   last_name="Dr. Lilienthal",
                   title="",
                   birthday=date(1848, 5, 23))
        self.assertEqual("Otto", p.first_name)
        self.assertEqual("Dr. Lilienthal", p.last_name)
        self.assertEqual("", p.title)
        self.assertEqual(date(1848, 5, 23), p.birthday)
        self.assertEqual(1, p.count)

        p = Person(first_name="Otto (3)", last_name="Lilienthal")
        self.assertEqual("Otto", p.first_name)
        self.assertEqual("Lilienthal", p.last_name)
        self.assertEqual(3, p.count)

    def test_fields(self):
        self.assertSetEqual({"first_name",
                             "last_name",
                             "title",
                             "comments",
                             "birthday",
                             "birthplace",
                             "count",
                             "uid",
                             "kind"}, Person.fields())

        self.assertSetEqual(set(), Person.nested_records())

    def test_from_dict(self):
        d = {"first_name": "Otto",
             "last_name": "Lilienthal",
             "kind": "male",
             "count": None,
             "age": "180"}
        p = Person.from_dict(d)
        self.assertIsInstance(p, Person)
        self.assertEqual("Otto", p.first_name)
        self.assertEqual("Lilienthal", p.last_name)
        self.assertIsNone(p.kind)

        common_fields = Person.fields_in(d.keys())
        p = Person.from_dict(d, common_fields)
        self.assertIsInstance(p, Person)
        self.assertEqual("Otto", p.first_name)
        self.assertEqual("Lilienthal", p.last_name)
        self.assertEqual(fsgop.db.person.PERSON_MALE, p.kind)

    def test_from_namedtuple(self):
        TupleType = namedtuple("TupleType",
                               ["first_name", "last_name", "age", "kind"])
        t = TupleType("Otto", "Lilienthal", 45, "male")

        recognised_fields = Person.fields_in(TupleType._fields)
        p = Person.from_obj(t, ["first_name", "last_name"])
        self.assertIsInstance(p, Person)
        self.assertEqual("Otto", p.first_name)
        self.assertEqual("Lilienthal", p.last_name)
        self.assertIsNone(p.kind)

        p = Person.from_obj(t, recognised_fields)
        self.assertIsInstance(p, Person)
        self.assertEqual("Otto", p.first_name)
        self.assertEqual("Lilienthal", p.last_name)
        self.assertEqual(fsgop.db.person.PERSON_MALE, p.kind)

    def test_username(self):
        p = Person()
        self.assertEqual("", p.username)

        p = Person(last_name="Ronaldo")
        self.assertEqual("ronaldo", p.username)

        p = Person(first_name="Christiano", last_name="Ronaldo")
        self.assertEqual("christiano.ronaldo", p.username)

        p = Person(first_name="Christiano", last_name="Ronaldo", count=7)
        self.assertEqual("christiano.ronaldo_7", p.username)

        p = Person(first_name="Sören (3)", last_name="Prof. Dr. O'Brian")
        self.assertEqual("soeren.obrian_3", p.username)

    def test_comparison(self):
        p1 = Person(first_name="Otto", last_name="Lilienthal")
        p2 = Person(first_name="Otto", last_name="Prof. Dr. Lilienthal")
        p3 = Person(first_name="Otto B.", last_name="Prof. Dr. Lilienthal")
        self.assertFalse(p1 < p2)
        self.assertFalse(p2 < p1)
        self.assertTrue(p1 < p3)
        self.assertEqual(p1, p2)
        self.assertNotEqual(p1, p3)


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(PersonTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )