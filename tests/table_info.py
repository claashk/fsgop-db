#!/usr/bin/env python3

import unittest
from datetime import datetime
from fsgop.db import TableInfo, ColumnInfo


class Empty(object):
    pass


class TableInfoTestCase(unittest.TestCase):
    @staticmethod
    def get_columns():
        return [
            ColumnInfo(name="col1", dtype="integer"),
            ColumnInfo(name="col2", dtype="float"),
            ColumnInfo(name="col3", dtype="time")
        ]

    def test_column_construction(self):
        col = ColumnInfo()
        self.assertIsNone(col.name)
        self.assertIsNone(col.dtype)
        self.assertFalse(col.allows_null)
        self.assertIsNone(col.index)
        self.assertIsNone(col.default_value)
        self.assertEqual('', col.extra)

    def test_is_primary_index(self):
        col = ColumnInfo()
        self.assertFalse(col.is_primary_index())
        col.index = "PRI"
        self.assertTrue(col.is_primary_index())

    def test_table_construction(self):
        t = TableInfo()
        self.assertEqual(0, t.ncols)

        t1 = TableInfo(self.get_columns())
        self.assertEqual(3, t1.ncols)

    def test_format(self):
        t = TableInfo()
        self.assertEqual("()", t.format())

        t1 = TableInfo(self.get_columns())
        self.assertEqual("(%s,%s,%s)", t1.format())

    def test_get_column(self):
        t = TableInfo(self.get_columns())
        c = t.get_column("col1")
        self.assertEqual("col1", c.name)
        self.assertEqual("integer", c.dtype)

        c = t.get_column("col3")
        self.assertEqual("col3", c.name)
        self.assertEqual("time", c.dtype)

        self.assertRaises(KeyError, t.get_column, "col4")

    def test_tuple_conversion(self):
        t = TableInfo(self.get_columns())
        rec = Empty()
        rec.col1 = 1
        rec.col4 = 42
        rec.col3 = datetime(2012, 1, 23, 14, 15, 16)
        rec.col2 = 2.1
        self.assertTupleEqual((1, 2.1, datetime(2012, 1, 23, 14, 15, 16)),
                              t.get_record(rec))


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TableInfoTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )