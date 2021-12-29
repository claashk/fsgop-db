#!/usr/bin/env python3
import unittest


class MysqlDatabaseTestCase(unittest.TestCase):
    def test_nothing(self):
        # TODO
        pass


def suite():
    """Get Test suite object
    """
    return unittest.TestLoader().loadTestsFromTestCase(MysqlDatabaseTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )