#!/usr/bin/env python3

import unittest
from datetime import datetime
from fsgop.db import TableInfo, ColumnInfo, IndexInfo


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


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TableInfoTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )