#!/usr/bin/env python3

import unittest
from pathlib import Path
from datetime import date

from fsgop.db import TableInfo
from fsgop.db.startkladde import schema_v3 as vz_schema
from fsgop.db import MysqlDatabase, Person

TEST_DIR = Path(__file__).parent


class MysqlDatabaseTestCase(unittest.TestCase):
    def test_import_person_from_dump_file(self):
        schema = {k: TableInfo.from_list(k, **v) for k, v in vz_schema.items()}
        db = MysqlDatabase(schema=schema)
        recs = list(db.iter_dump_file(TEST_DIR / "mysql-dump.tsv",
                                      table="people",
                                      aliases={"medical_validity": "birthday"}))
        self.assertEqual(3, len(recs))
        self.assertEqual("Otto", recs[0].first_name)
        self.assertEqual(date(2022, 4, 7), recs[0].birthday)

        self.assertEqual("Einstein", recs[1].last_name)
        self.assertEqual(1, recs[1].check_medical_validity)
        self.assertEqual("FSG-HH", recs[1].club)
        self.assertEqual("Newton", recs[2].last_name)

        attrs = Person.fields_in(type(recs[0])._fields)
        self.assertSetEqual({"first_name", "last_name", "birthday", "comments"},
                            attrs)
        persons = [Person.from_obj(rec, attrs=attrs) for rec in recs]
        self.assertEqual(3, len(persons))
        self.assertEqual("Otto", persons[0].first_name)
        self.assertEqual("Lilienthal", persons[0].last_name)
        self.assertIsNone(persons[0].uid)


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(MysqlDatabaseTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )