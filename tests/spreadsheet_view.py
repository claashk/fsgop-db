#!/usr/bin/env python3

import unittest
from pathlib import Path

from fsgop.db import SpreadsheetView, SqliteDatabase, Repository
from fsgop.db.startkladde import schema_v3 as sk_schema


DATA_DIR = Path(__file__).parent / "startkladde-dump"


class SpreadsheetViewTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db = DATA_DIR.parent / "spreadsheet_view_tmp.sqlite3"
        cls.db_path = DATA_DIR.parent / "spreadsheet_view_db.sqlite3"
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
        self.csv_path = DATA_DIR.parent / "spreadsheet_view_test.csv"

    def tearDown(self) -> None:
        if not self.keep_artifacts:
            for p in (self.csv_path, ):
                try:
                    p.unlink()
                except FileNotFoundError:
                    pass

    def test_csv_export(self):
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
        view(self.repo.add("vehicle_properties",
                           self.repo.read("missions", order="missions.begin")),
             self.csv_path)


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(SpreadsheetViewTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
