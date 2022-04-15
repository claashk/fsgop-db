#!/usr/bin/env python3

import unittest
from fsgop.db import Person, PersonProperty, NameAdapter
from fsgop.db.utils import to
from fsgop.db.person import split_title, split_count

from datetime import date, datetime
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

    def test_layout(self):
        layout = {
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
        self.assertDictEqual(layout, Person.layout())

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

    def test_name(self):
        p = Person(first_name="Christiano", last_name="Ronaldo", count=7)
        self.assertEqual("Ronaldo, Christiano (7)", p.name)

        p = Person(last_name="Ronaldo")
        self.assertEqual("Ronaldo", p.name)

        p = Person(first_name="Twiggy")
        self.assertEqual("Twiggy", p.name)


    def test_comparison(self):
        p1 = Person(first_name="Otto", last_name="Lilienthal")
        p2 = Person(first_name="Otto", last_name="Prof. Dr. Lilienthal")
        p3 = Person(first_name="Otto B.", last_name="Prof. Dr. Lilienthal")
        self.assertFalse(p1 < p2)
        self.assertFalse(p2 < p1)
        self.assertTrue(p1 < p3)
        self.assertEqual(p1, p2)
        self.assertNotEqual(p1, p3)

        p4 = Person(uid=5, first_name="Otto", last_name="Lilienthal")
        # names should be preferred over uid when matching
        self.assertTrue(p4 == p1)
        self.assertTrue(p4 == p2)
        self.assertTrue(p4 < p3)
        self.assertEqual(("Lilienthal", "Otto", 1), p4)  # key comparison

        p5 = Person(uid=5)
        self.assertEqual(p5, 5)  # key comparison

        # ideally the next line should raise, but an equal comparison between
        # tuples and integers is possible, while a less comparison is not
        self.assertFalse(p4 == p5)
        with self.assertRaises(TypeError):
            _x = p4 < p5

        p4.first_name = ""
        p4.last_name = ""
        self.assertEqual(p4, p5)  # comparison by uid if index cannot be created

    def test_hash(self):
        p1 = Person(first_name="Otto", last_name="Lilienthal")
        p2 = Person(first_name="Otto", last_name="Prof. Dr. Lilienthal")
        p3 = Person(first_name="Otto B.", last_name="Prof. Dr. Lilienthal")
        p4 = Person()

        s = {p1, p2, p3, p4}
        self.assertEqual(3, len(s))

        s.add(None)
        self.assertEqual(3, len(s))  # None equals empty Record

        result = []
        for p in s:
            if p is not p4:
                self.assertTrue(p)
                result.append(1)
            else:
                self.assertFalse(p)
                result.append(0)
        self.assertEqual(2, result.count(1))
        self.assertEqual(1, result.count(0))

    def test_uid_construction(self):
        p = to(Person, 5)
        self.assertEqual(5, p.uid)
        self.assertEqual(5, int(p))

    def test_properties(self):
        person = Person(first_name="Otto", last_name="Lilienthal")
        spl = PersonProperty(kind="Licences", value="SPL 123456")
        ppl = PersonProperty(kind="Licences", value="PPL 321645")
        m1 = PersonProperty(kind="Membership",
                            value="Club1 (regular)",
                            valid_from=datetime(1900, 5, 1),
                            valid_until=datetime(1930, 1, 1))
        m2 = PersonProperty(kind="Membership",
                            value="Club1 (honorary)",
                            valid_from=datetime(1930, 1, 1))
        self.assertFalse(person.has_properties)
        spl.add_to(person)
        self.assertTrue(person.has_properties)
        PersonProperty.discard_from(person, spl.kind)
        self.assertFalse(person.has_properties)

        spl.add_to(person)
        ppl.add_to(person)
        ppl.add_to(person)
        m1.add_to(person)
        m1.add_to(person)
        m2.add_to(person)

        self.assertEqual(2, len(person["Licences"]))
        for p in person["Licenses"]:
            self.assertIsInstance(p, PersonProperty)
            self.assertIs(p.rec, person)

        self.assertEqual(2, len(person["Membership"]))
        p = PersonProperty.get_from(person,
                                    "Membership",
                                    at=datetime(1905, 5, 4))
        self.assertIs(p.rec, person)
        self.assertEqual("Club1 (regular)", p.value)

        p = PersonProperty.get_from(person,
                                    "Membership",
                                    at=datetime(1930, 1, 1))
        self.assertIs(p.rec, person)
        self.assertEqual("Club1 (honorary)", p.value)

        person["Membership"].discard(p)
        self.assertEqual(1, len(person["Membership"]))

    def test_property_layout(self):
        layout = {"uid": "uid",
                  "person": Person.layout(prefix="person_"),
                  "valid_from": "valid_from",
                  "valid_until": "valid_until",
                  "kind": "kind",
                  "value": "value"}

        self.assertDictEqual(layout, PersonProperty.layout())

    def test_split_names(self):
        Rec = namedtuple("Rec",
                         ["pilot_name",
                          "copilot_last_name",
                          "copilot_first_name",
                          "passenger_last_name",
                          "passenger_name"])

        records = [
            Rec("Lindberg, Charles", "Flyer", "Victor", "Pax", "Pax, Herbert"),
            Rec("Sky", "Henry", "Crash", "", "Chase")
        ]
        recs, rec_type = NameAdapter.apply(records)
        self.assertEqual("ModifiedRec", rec_type.__name__)
        self.assertTupleEqual(rec_type._fields,
                              ("copilot_last_name",
                               "copilot_first_name",
                               "pilot_first_name",
                               "pilot_last_name",
                               "passenger_first_name",
                               "passenger_last_name"))

        output_records = list(recs)
        self.assertEqual("Charles", output_records[0].pilot_first_name)
        self.assertEqual("Flyer", output_records[0].copilot_last_name)
        self.assertEqual("Pax", output_records[0].passenger_last_name)
        self.assertEqual("", output_records[1].pilot_first_name)
        self.assertEqual("Sky", output_records[1].pilot_last_name)
        self.assertEqual("Chase", output_records[1].passenger_last_name)


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(PersonTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )