#!/usr/bin/env python3

import unittest
from collections import namedtuple

from fsgop.db import NameAdapter, DateTimeAdapter


class TupleAdapterTestCase(unittest.TestCase):
    def test_name_adapter(self):
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
        adapter = NameAdapter()
        recs, rec_type = adapter.apply_to(records)
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

    def test_datetime_adapter(self):
        Rec = namedtuple("Rec",
                         ["launch_location",
                          "begin_date",
                          "begin_time",
                          "end_date",
                          "end_time",
                          "landing_location"])

        records = [
            Rec("EDDF", "2021-12-14", "13:22", "2021-12-15", "00:01", "EDDM"),
            Rec("EDDM", "2022-01-01", "23:55", "2022-01-02", "01:01", "EDDF")
        ]
        adapter = DateTimeAdapter()
        recs, rec_type = adapter.apply_to(records)
        self.assertEqual("ModifiedRec", rec_type.__name__)
        self.assertTupleEqual(rec_type._fields, ("launch_location",
                                                 "landing_location",
                                                 "begin",
                                                 "end"))
        output_records = list(recs)
        self.assertEqual("EDDF", output_records[0].launch_location)
        self.assertEqual("EDDM", output_records[0].landing_location)
        self.assertEqual("2021-12-14 13:22", output_records[0].begin)
        self.assertEqual("2021-12-15 00:01", output_records[0].end)

        self.assertEqual("EDDM", output_records[1].launch_location)
        self.assertEqual("EDDF", output_records[1].landing_location)
        self.assertEqual("2022-01-01 23:55", output_records[1].begin)
        self.assertEqual("2022-01-02 01:01", output_records[1].end)

    def test_datetime_adapter_with_single_date(self):
        Rec = namedtuple("Rec",
                         ["launch_location",
                          "begin_date",
                          "begin_time",
                          "end_time",
                          "landing_location"])

        records = [
            Rec("EDDF", "2021-12-14", "13:22", "00:01", "EDDM"),
            Rec("EDDM", "2022-01-01", "23:55", "01:01", "EDDF")
        ]
        adapter = DateTimeAdapter(delimiter="T")
        recs, rec_type = adapter.apply_to(records)
        self.assertEqual("ModifiedRec", rec_type.__name__)
        self.assertTupleEqual(rec_type._fields, ("launch_location",
                                                 "landing_location",
                                                 "begin",
                                                 "end"))
        output_records = list(recs)
        self.assertEqual("EDDF", output_records[0].launch_location)
        self.assertEqual("EDDM", output_records[0].landing_location)
        self.assertEqual("2021-12-14T13:22", output_records[0].begin)
        self.assertEqual("2021-12-14T00:01", output_records[0].end)

        self.assertEqual("EDDM", output_records[1].launch_location)
        self.assertEqual("EDDF", output_records[1].landing_location)
        self.assertEqual("2022-01-01T23:55", output_records[1].begin)
        self.assertEqual("2022-01-01T01:01", output_records[1].end)


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(TupleAdapterTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )