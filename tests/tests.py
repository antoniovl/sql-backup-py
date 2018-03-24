#!/usr/bin/env python

import unittest
import sqlbackup


class SQLBackupTest(unittest.TestCase):
    def test__get_file_name(self):
        tool = sqlbackup.SQLBackup()
        tool.load_config('tests/test-config.json')
        file_name = tool.get_file_name(sqlbackup.DBType.MYSQL, 'test_db')
        self.assertEqual('mysql-test_db.bak', file_name)


if __name__ == '__main__':
    unittest.main()
