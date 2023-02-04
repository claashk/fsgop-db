#!/usr/bin/env python3

import unittest
from datetime import datetime, date
from pathlib import Path

from fsgop.db import TableInfo, ColumnInfo, IndexInfo, Person, sort_tables
from fsgop.db.table_info import dependencies
from fsgop.db import SchemaIterator
from fsgop.db import to_schema
from fsgop.db.startkladde import schema_v3
from fsgop.db.native_schema import tables
from fsgop.db.utils import kwargs_from


TEST_DIR = Path(__file__).parent / "test-data"


class TableInfoTestCase(unittest.TestCase):
    @staticmethod
    def get_columns():
        return [
            ColumnInfo(name="col1", dtype="integer"),
            ColumnInfo(name="col2", dtype="float"),
            ColumnInfo(name="col3", dtype="time")
        ]

    @staticmethod
    def get_indices():
        return [
            IndexInfo("secondary", False, False, [("col2", 1, 1),
                                                  ("col1", -1, 0),
                                                  ("col3", 0, 2)]),
            IndexInfo("primary", True, True, [("col2", 1, 1), ("col1", -1, 0)])
        ]

    @staticmethod
    def get_schema(native=False):
        return to_schema(tables) if native else to_schema(schema_v3)

    def test_column_construction(self):
        col = ColumnInfo()
        self.assertIsNone(col.name)
        self.assertIsNone(col.dtype)
        self.assertFalse(col.allows_null)
        self.assertIsNone(col.default_value)
        self.assertEqual('', col.extra)

    def test_table_construction(self):
        t = TableInfo()
        self.assertEqual(0, t.ncols)

        t1 = TableInfo(columns=self.get_columns())
        self.assertEqual(3, t1.ncols)

        t2 = TableInfo(columns=self.get_columns(), indices=self.get_indices())
        self.assertEqual(2, t2.nidx)
        self.assertSetEqual(set(["primary", "secondary"]), t2.index_names)

    def test_format(self):
        t = TableInfo()
        self.assertEqual("()", t.format())

        t1 = TableInfo(columns=self.get_columns())
        self.assertEqual("(%s,%s,%s)", t1.format())

    def test_get_column(self):
        t = TableInfo(columns=self.get_columns())
        c = t.get_column("col1")
        self.assertEqual("col1", c.name)
        self.assertEqual("integer", c.dtype)

        c = t.get_column("col3")
        self.assertEqual("col3", c.name)
        self.assertEqual("time", c.dtype)

        self.assertRaises(KeyError, t.get_column, "col4")

    def test_equals(self):
        t1 = TableInfo(columns=self.get_columns())
        t2 = TableInfo(columns=self.get_columns())
        t3 = TableInfo(columns=self.get_columns(), indices=self.get_indices())

        self.assertEqual(t1, t1)
        self.assertEqual(t1, t2)
        self.assertNotEqual(t2, t3)
        self.assertEqual(t3, t3)

        for idx in self.get_indices():
            t2.add_index(idx)

        self.assertEqual(t2, t3)
        self.assertNotEqual(t1, t2)

    def test_tuple_conversion(self):
        t = TableInfo(name="Table", columns=self.get_columns())
        aliases = dict(col1="first", col3="third")
        t.reset_record_type(aliases=aliases)

        d1 = dict(first=1,
                  third=datetime(2012, 1, 23, 14, 15, 16),
                  col2=2.1)

        rec = t.record_type(**d1)
        self.assertTupleEqual((1, 2.1, datetime(2012, 1, 23, 14, 15, 16)),
                              rec)

    def test_get_primary_key(self):
        t = TableInfo(columns=self.get_columns(), indices=self.get_indices())
        self.assertEqual("PRIMARY KEY (col1 DESC,col2)", t.primary_key())

    def test_id_column(self):
        schema = self.get_schema()
        for table in ("people", "flights", "launch_methods"):
            self.assertEqual("id", schema[table].id_column)

        info = TableInfo(columns=self.get_columns(), indices=self.get_indices())
        self.assertIsNone(info.id_column)

    def test_import_mysql_dump(self):
        schema = self.get_schema()
        recs = list(schema["people"].read_mysql_dump(
                        TEST_DIR / "mysql-dump.tsv",
                        aliases={"medical_validity": "birthday"}))
        self.assertEqual(3, len(recs))
        self.assertEqual("Otto", recs[0].first_name)
        self.assertEqual(date(2022, 4, 7), recs[0].birthday)

        self.assertEqual("Einstein", recs[1].last_name)
        self.assertEqual(1, recs[1].check_medical_validity)
        self.assertEqual("FSG-HH", recs[1].club)
        self.assertEqual("Newton", recs[2].last_name)

        layout = Person.layout(allow=type(recs[0])._fields)
        self.assertSetEqual({"first_name", "last_name", "birthday", "comments"},
                            set(layout.keys()))
        persons = [Person(**kwargs_from(rec, layout=layout)) for rec in recs]
        self.assertEqual(3, len(persons))
        self.assertEqual("Otto", persons[0].first_name)
        self.assertEqual("Lilienthal", persons[0].last_name)
        self.assertIsNone(persons[0].uid)

    def test_references(self):
        schema = self.get_schema()
        refs = schema["people"].get_references()
        self.assertFalse(refs)

        refs = schema["flights"].get_references()
        self.assertDictEqual({"people": {"id"},
                              "planes": {"id"},
                              "launch_methods": {"id"}},
                             refs)

        for k in refs.keys():
            self.assertIn(k, {"people", "planes", "launch_methods"})

    def test_sort(self):
        schema = self.get_schema()
        sorted_tables = sort_tables(schema.values())
        self.assertListEqual(["launch_methods",
                              "people",
                              "planes",
                              "schema_migrations",
                              "flights"],
                             [t.name for t in sorted_tables])

    def test_schema_iterator(self):
        schema = self.get_schema()
        traverse = SchemaIterator(schema)
        aliases = {"plane_id": "plane",
                   "towplane_id": "towplane",
                   "pilot_id": "pilot_",
                   "copilot_id": "copilot_"}

        fields = ["_".join(aliases.get(s, s) for s in it.path)
                  for it in traverse("flights")]

        for x in ["plane_id",
                  "plane_registration",
                  "pilot_first_name",
                  "pilot__id",
                  "pilot__first_name",
                  "pilot__last_name",
                  "copilot__last_name",
                  "copilot__id"]:
            self.assertIn(x, fields)

    def test_schema_iterator_parent(self):
        schema = self.get_schema()
        traverse = SchemaIterator(schema)

        refs = {it.parent(): it.unique_name for it in traverse("flights")
                if it.parent() is not None}

        for col in schema["flights"]:
            rtable, rcol = col.ref_info
            if rtable is not None:
                colname = f"flights.{col.name}"
                self.assertEqual(f"{col.name}_{rtable}.{rcol}",
                                 refs[colname])
            else:
                self.assertNotIn(col.name, refs.keys())

        schema = self.get_schema(native=True)
        traverse = SchemaIterator(schema)
        refs = {it.parent(): it.unique_name for it in traverse("missions", 2)
                if it.parent() is not None}

        for col in schema["missions"]:
            rtable, rcol = col.ref_info
            if rtable is not None:
                colname = f"missions.{col.name}"
                self.assertEqual(f"{col.name}_{rtable}.{rcol}", refs[colname])

                # check recursive launch items
                colname = f"launch_missions.{col.name}"
                self.assertEqual(f"launch_{col.name}_{rtable}.{rcol}",
                                 refs[colname])
            else:
                self.assertNotIn(col.name, refs.keys())

    def test_dependencies(self):
        schema = self.get_schema(native=True)
        result = dict()
        for col, ref_table, ref_col in dependencies(schema, "people"):
            result.setdefault(col, set()).add((ref_table, ref_col))

        self.assertEqual(1, len(result))
        refs = result['uid']
        self.assertIn(("missions", "charge_person"), refs)
        self.assertIn(("missions", "pilot"), refs)
        self.assertIn(("missions", "copilot"), refs)
        for i in range(4):
            self.assertIn(("missions", f"passenger{i+1}"), refs)
        self.assertIn(("person_properties", "person"), refs)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TableInfoTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )