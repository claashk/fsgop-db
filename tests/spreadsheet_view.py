#!/usr/bin/env python3

import unittest
from pathlib import Path

from fsgop.db import SpreadsheetView, SqliteDatabase, Repository
from fsgop.db import Vehicle, Mission
from fsgop.db.startkladde import schema_v3 as sk_schema


DATA_DIR = Path(__file__).parent / "test-data" / "startkladde-dump"


class SpreadsheetViewTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        odir = DATA_DIR.parent.parent / "artifacts"
        cls.temp_db = odir / "spreadsheet_view_tmp.sqlite3"
        cls.db_path = odir / "spreadsheet_view_db.sqlite3"
        with SqliteDatabase.from_dump(DATA_DIR,
                                      schema=sk_schema,
                                      db=str(cls.temp_db)) as db:
            pass
        cls.repo = Repository.from_startkladde(cls.temp_db, cls.db_path)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.repo.close()
        for p in (cls.temp_db, cls.db_path):
            try:
                p.unlink()
            except FileNotFoundError:
                pass

    def setUp(self) -> None:
        self.keep_artifacts = True
        self.csv_path = self.db_path.parent / "spreadsheet_view_test.csv"
        self.xlsx_path = self.csv_path.with_suffix(".xlsx")

    def tearDown(self) -> None:
        if not self.keep_artifacts:
            for p in (self.csv_path, self.xlsx_path):
                try:
                    p.unlink()
                except FileNotFoundError:
                    pass

    def test_export(self):
        columns = [
            "pilot.last_name",
            "pilot.first_name",
            "begin",
            "end",
            "origin",
            "destination",
            "vehicle.registration",
            "comments"
        ]

        fmt = {
            "begin": SpreadsheetView.date_formatter("%Y-%m-%d %H:%M"),
            "end": SpreadsheetView.date_formatter("%Y-%m-%d %H:%M")
        }

        view = SpreadsheetView(columns=columns, fmt=fmt)
        for path in (self.csv_path, self.xlsx_path):
            view.path = path
            view(self.repo.add("vehicle_properties",
                               self.repo.read("missions",
                                              order="missions.begin,"
                                                    "missions.end")))
            #TODO -> need some checks here

    def test_for_record(self):
        cols = list(SpreadsheetView.cols_from_layout(Vehicle.layout()))
        self.assertListEqual(["uid",
                              "manufacturer",
                              "model",
                              "serial_number",
                              "num_seats",
                              "category",
                              "registration",
                              "comments"],
                             cols)
        mission = Mission()
        view = SpreadsheetView.for_record(mission)
        self.assertIn("begin", view.columns)
        self.assertIn("end", view.columns)
        self.assertIn("pilot.first_name", view.columns)
        self.assertIn("copilot.last_name", view.columns)
        self.assertIn("launch.uid", view.columns)


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(SpreadsheetViewTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
