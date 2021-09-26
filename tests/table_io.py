from fsgop.db import CsvParser
from fsgop.db.table_io import WITH_XLSX_SUPPORT

import unittest
from pathlib import Path


class TableIOTestCase(unittest.TestCase):
    def create_input(self, lines, newline="\n"):
        with open(self.csv_path, mode='w', newline='\n') as ofile:
            ofile.write(newline.join(lines))

    def setUp(self):
        """Set up test parameters
        """
        # Make sure path works for different CWDs
        self.test_dir = Path(__file__).parent

    def test_startkladde_csv(self):
        reader = CsvParser(headings=["Pilot Vorname", "Pilot Nachname"],
                           force_lowercase=True,
                           translation={"Pilot_Vorname": "pilot_first_name",
                                        "Pilot_Nachname": "pilot_last_name"})
        retval = list(reader(self.test_dir / "startkladde-format.csv"))
        expected_columns = [
            "datum",
            "nummer",
            "kennzeichen",
            "typ",
            "flugzeug_verein",
            "pilot_last_name",
            "pilot_first_name",
            "pilot_verein",
            "pilot_vid",
            "begleiter_nachname",
            "begleiter_vorname",
            "begleiter_verein",
            "begleiter_vid",
            "flugtyp",
            "anzahl_landungen",
            "modus",
            "startzeit",
            "landezeit",
            "flugdauer",
            "startart",
            "kennzeichen_schleppflugzeug",
            "modus_schleppflugzeug",
            "landung_schleppflugzeug",
            "startort",
            "zielort",
            "zielort_schleppflugzeug",
            "bemerkungen",
            "abrechnungshinweis",
            "dbid"
        ]
        self.assertListEqual(expected_columns, list(reader.row_type._fields))
        self.assertEqual("Lilienthal", retval[0].pilot_last_name)
        self.assertEqual("Harald", retval[1].pilot_first_name)

    def test_excel(self):
        if not WITH_XLSX_SUPPORT:
            self.skipTest("openpyxl library not found")

        parser = CsvParser(headings=["Heading1"],
                           header={
                               "int": r"int header field (\d+)",
                               "float": r"float header field (\d+.\d*)"})

        retval = list(parser.parse(self.test_dir / "simple-test.xlsx"))

        self.assertEqual(len(retval), 3)
        self.assertEqual(retval[0].Heading1, "one")
        self.assertEqual(retval[0].Heading2, "1")
        self.assertEqual(retval[0].Heading3, "0.1")
        self.assertEqual(retval[1].Heading1, "two")
        self.assertEqual(retval[1].Heading2, "2")
        self.assertEqual(retval[1].Heading3, "0.2")
        self.assertEqual(retval[2].Heading1, "three")
        self.assertEqual(retval[2].Heading2, "3")
        self.assertEqual(retval[2].Heading3, "0.3")

    def test_mysql_dump(self):
        reader = CsvParser()
        retval = list(reader(self.test_dir / "mysql-dump.tsv",
                             skip_rows=0,
                             delimiter="\t"))
        self.assertEqual(3, len(retval))
        self.assertIsInstance(retval[0], tuple)
        self.assertListEqual([1,2,3], [int(x[0]) for x in retval])
        self.assertEqual("Lilienthal", retval[0][1])
        self.assertEqual("Albert", retval[1][2])
        self.assertEqual(r"\N", retval[2][4])
        self.assertEqual("FSG Cambridge", retval[2][3])


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TableIOTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
