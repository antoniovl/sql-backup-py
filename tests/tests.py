#!/usr/bin/env python

import unittest

from schematics.exceptions import DataError

import sqlbackup
from common import SQLBackupConfig, SQLBackupError, DBType, CompressionTypeEnum, Frequency, Database, DBServer


class SQLBackupTest(unittest.TestCase):
    def test__get_file_name(self):
        tool = sqlbackup.SQLBackup('tests/test-config.json')
        file_name = tool.get_file_name(DBType.MYSQL.value, 'test_db')
        self.assertEqual('/tmp/mysql-test_db.sql', file_name)

    def test_database_model(self):
        configs = [
            {},
            {
                "db_name": "name1",
                "frequency": "wrong_freq",
                "compress": True,
                "verify": True
            },
            {
                "frequency": "daily",
                "compress": True,
                "verify": True
            },
            {
                "db_name": "name1",
                "frequency": "daily",
                "compress": 100,
                "verify": True
            }
        ]
        for config in configs:
            with self.assertRaises(Exception) as context:
                d = Database(config)
                d.validate()
            self.assertTrue(isinstance(context.exception, DataError))
        # Should pass ok
        d = Database({
            "db_name": "name1",
            "frequency": "daily",
            "compress": True,
            "verify": True
        })
        d.validate()

    def test_db_server_model(self):
        config = {
            "db_type": "postgresql",
            "hostname": "kathan.ka.icon.mx",
            "port": 5630,
            "user": "user1",
            "password": "s3cret1",
            "databases": [
                {
                    "db_name": "sql_backup_db",
                    "frequency": "weekly",
                    "compress": True,
                    "verify": True
                }
            ]
        }
        db_server = DBServer(config)
        db_server.validate()


if __name__ == '__main__':
    unittest.main()
