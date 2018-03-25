#!/usr/bin/env python

import logging
import json
import argparse
import copy
import subprocess
import os
import sys
import traceback
import datetime, time

from enum import Enum


CFG_DAY_OF_WEEK = 'day_of_week'
CFG_DAY_OF_MONTH = 'day_of_month'
CFG_DATA_DIR = 'data_dir'
CFG_LOG_FILE = 'log_file'
CFG_LOG_MODE = 'log_mode'
CFG_LOG_LEVEL = 'log_level'
CFG_TIMESTAMPS = 'timestamps'
CFG_MYSQL_DUMP_EXE = 'mysql_dump_exe'
CFG_COMPRESSION_TYPE = 'compression_type'
CFG_BZIP2_EXE = 'bzip2_exe'
CFG_P7ZIP_EXE = 'p7zip_exe'
CFG_GZIP_EXE = 'gzip_exe'
CFG_PG_DUMP_EXE = 'pg_dump_exe'
CFG_DB_SERVERS = 'db_servers'
CFG_DATABASES = 'databases'

SERVER_USER = 'user'
SERVER_PASS = 'password'
SERVER_HOST = 'hostname'
SERVER_PORT = 'port'
SERVER_DATABASES = 'databases'
SERVER_TYPE = 'type'

DB_NAME = 'db_name'
DB_COMPRESS = 'compress'
DB_FREQUENCY = 'frequency'
DB_VERIFY = 'verify'

DAYS_OF_WEEK = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']
LOG_MODES = ['append', 'overwrite']

DEFAULT_CONFIG = {
    CFG_DAY_OF_WEEK: 'sunday',
    CFG_DAY_OF_MONTH: 1,
    CFG_DATA_DIR: 'baks',
    CFG_LOG_FILE: 'sql-back.log',
    CFG_LOG_MODE: 'append',
    CFG_LOG_LEVEL: 'DEBUG',
    CFG_TIMESTAMPS: True,
    CFG_MYSQL_DUMP_EXE: '/opt/local/bin/mysqldump',
    CFG_BZIP2_EXE: '/usr/bin/bzip2',
    CFG_P7ZIP_EXE: '7z-ultra.sh',
    CFG_PG_DUMP_EXE: '/opt/local/bin/pg_dump',
    CFG_DB_SERVERS: {}
}

# _config_schema = {
#     '$schema': 'http://json-schema.org/schema#',
#     'description': 'SQL Backup configuration file schema',
#     CFG_DAY_OF_WEEK: {
#         'type': 'string',
#         'enum': DAYS_OF_WEEK
#     },
#     CFG_DAY_OF_MONTH: {
#         'type': 'integer',
#         'minimum': 1,
#         'maximum': 7
#     },
#     CFG_DATA_DIR: {
#         'type': 'string',
#     },
#     CFG_LOG_FILE: {
#         'type': 'string',
#         'default': 'sql-backup.log'
#     },
#     CFG_LOG_MODE: {
#         'type': 'string',
#         'enum': LOG_MODES
#     },
#     CFG_LOG_LEVEL: {
#         'type': 'string'
#     },
#     CFG_TIMESTAMPS: {
#         'type': 'boolean'
#     },
#     CFG_MYSQL_DUMP_EXE: {
#         'type': 'string',
#         'default': '/usr/bin/mysqldump'
#     },
#     CFG_PG_DUMP_EXE: {
#         'type': 'string',
#         'default': '/usr/bin/pgdump'
#     },
#     CFG_BZIP2_EXE: {
#         'type': 'string',
#         'default': '/usr/bin/bzip2'
#     },
#     CFG_P7ZIP_EXE: {
#         'type': 'string',
#         'default': '/usr/bin/7z'
#     },
#     'required': [CFG_DATA_DIR]
# }

ENV_MYSQL_PWD = 'MYSQL_PWD'
ENV_PGPASSWORD = 'PGPASSWORD'


logger = logging.getLogger(__name__)


class DBType(Enum):
    """Database type enumeration."""
    MYSQL = 'mysql'
    POSTGRESQL = 'postgresql'


class Frequency(Enum):
    """Frequencies enumeration."""
    MONTHLY = 'monthly'
    WEEKLY = 'weekly'
    DAILY = 'daily'


class LogWriteMode(Enum):
    APPEND = 'append'
    CREATE = 'create'


class CompressionTypeEnum(Enum):
    BZ2 = 'bz2'
    GZ = 'gz'
    P7Z = '7z'


class SQLBackupError(Exception):
    pass


class SQLBackup(object):

    def __init__(self):
        self._config = copy.deepcopy(DEFAULT_CONFIG)
        self._logger = logging.getLogger('')

    def load_config(self, config_file_name):
        """
        Loads the config from the json file.
        :param config_file_name:
        :return: Instance of dict with all the settings.
        """
        with open(config_file_name) as config_file:
            json_cfg = json.load(config_file)

        # jsonschema.validate(json_cfg, _config_schema, format_checker=jsonschema.FormatChecker())

        # Read values
        if json_cfg[CFG_DAY_OF_WEEK]:
            value = json_cfg[CFG_DAY_OF_WEEK]
            if value not in DAYS_OF_WEEK:
                raise ValueError("{} is not a valid day of week".format(value))
            DEFAULT_CONFIG[CFG_DAY_OF_WEEK] = value
        # Day of month
        if json_cfg[CFG_DAY_OF_MONTH]:
            value = json_cfg[CFG_DAY_OF_MONTH]
            if not type(value) == int:
                raise ValueError('Expected int value in day of month')

        self._config = json_cfg
        return self._config

    @staticmethod
    def _get_timestamp(clock):
        return time.strftime('%Y%m%d-%H%M', clock)

    def get_file_name(self, db_type, db_name):
        """
        Calculates que backup file name using the db type prefix and the timestamp (if required).
        """
        cfg = self._config
        data_dir = cfg[CFG_DATA_DIR]

        if db_type == DBType.MYSQL.value:
            t = 'mysql'
        else:
            t = 'pgsql'

        if cfg[CFG_TIMESTAMPS]:
            local_time = time.localtime()
            ts = self._get_timestamp(local_time)
            return '{}/{}-{}-{}.sql'.format(data_dir, t, db_name, ts)

        return '{}/{}-{}.sql'.format(data_dir, t, db_name)

    def compress_bz2(self, file_name):
        """
        Compresses the resultant file.
        We avoid using the python bzip2 modules because we need to compress the data
        inplace without reading or writing to memory.
        """
        cfg = self._config
        bzip_exe = cfg[CFG_BZIP2_EXE]
        cmd = [bzip_exe, '-9', file_name]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info('Compressing {}'.format(file_name))
        (stdout, stderr) = p.communicate()
        if stderr and len(stderr) > 0:
            raise SQLBackupError('Error compressing {}: {}'.format(file_name, stderr))

    def verify_bz2(self, file_name):
        """
        Verifies the bzip2 compressed file.
        """
        cfg = self._config
        bzip_exe = cfg[CFG_BZIP2_EXE]
        cmd = [bzip_exe, '-t', file_name]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info('Verifying {}'.format(file_name))
        (stdout, stderr) = p.communicate()
        if stderr and len(stderr) > 0:
            raise SQLBackupError('Integrity error in {}: {}'.format(file_name, stderr))

    def compress_gz(self, file_name):
        cfg = self._config
        gzip_exe = cfg[CFG_GZIP_EXE]
        cmd = [gzip_exe, '-9', file_name]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info('Compressing {}'.format(file_name))
        (stdout, stderr) = p.communicate()
        if stderr and len(stderr) > 0:
            raise SQLBackupError('Error compressing {}: {}'.format(file_name, stderr))

    def verify_gz(self, file_name):
        cfg = self._config
        gzip_exe = cfg[CFG_GZIP_EXE]
        cmd = [gzip_exe, '-t', file_name]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info('Verifying {}'.format(file_name))
        (stdout, stderr) = p.communicate()
        if stderr and len(stderr) > 0:
            raise SQLBackupError('Integrity error in {}: {}'.format(file_name, stderr))

    def compress_7z(self, file_name):
        cfg = self._config
        p7z_exe = cfg[CFG_P7ZIP_EXE]
        p7z_file = '{}.7z'.format(file_name)
        cmd = [p7z_exe, 'a', '-bd', '-t7z', '-m0=lzma', '-mx=9', '-mfb=64', '-md=64m', '-ms=on', '-sdel', p7z_file, file_name]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info('Compressing {}'.format(file_name))
        (stdout, stderr) = p.communicate()
        if stderr and len(stderr) > 0:
            raise SQLBackupError('Error compressing {}: {}'.format(file_name, stderr))

    def verify_7z(self, file_name):
        cfg = self._config
        p7z_exe = cfg[CFG_P7ZIP_EXE]
        cmd = [p7z_exe, 't', file_name]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info('Verifying {}'.format(file_name))
        (stdout, stderr) = p.communicate()
        if stderr and len(stderr) > 0:
            raise SQLBackupError('Integrity error in {}: {}'.format(file_name, stderr))

    def compress(self, file_name):
        cfg = self._config
        compression_type = cfg[CFG_COMPRESSION_TYPE]
        if compression_type == CompressionTypeEnum.BZ2.value:
            self.compress_bz2(file_name)
            compressed_file_name = '{}.{}'.format(file_name, CompressionTypeEnum.BZ2.value)
        elif compression_type == CompressionTypeEnum.GZ.value:
            self.compress_gz(file_name)
            compressed_file_name = '{}.{}'.format(file_name, CompressionTypeEnum.GZ.value)
        else:
            self.compress_7z(file_name)
            compressed_file_name = '{}.{}'.format(file_name, CompressionTypeEnum.P7Z.value)

        return compressed_file_name

    def verify(self, file_name):
        """
        Verifies the compressed file.
        """
        cfg = self._config
        compression_type = cfg[CFG_COMPRESSION_TYPE]
        if compression_type == CompressionTypeEnum.BZ2.value:
            self.verify_bz2(file_name)
        elif compression_type == CompressionTypeEnum.GZ.value:
            self.verify_gz(file_name)
        else:
            self.verify_7z(file_name)

    def mysql_backup(self, user, passwd, db, file_name, host='localhost', port=3306):
        cfg = self._config
        cmd = [
            cfg[CFG_MYSQL_DUMP_EXE], '--opt', '--single-transaction',
            '-u', user,
            '-h', host,
            '--port={}'.format(port),
            '--result-file', file_name,
            db
        ]

        os.environ[ENV_MYSQL_PWD] = passwd
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = p.communicate()
        del (os.environ[ENV_MYSQL_PWD])

        if stderr and len(stderr) > 0:
            raise SQLBackupError(stderr)

    def pgsql_backup(self, user, passwd, db, file_name, host='localhost', port=5432):
        cfg = self._config
        cmd = [
            cfg[CFG_PG_DUMP_EXE],
            '-h', host,
            '-p', str(port),
            '-U', user,
            '-Fp', '-b', '-O', '-E', 'UTF8', '-x',
            '--file={}'.format(file_name),
            db
        ]

        os.environ[ENV_PGPASSWORD] = passwd
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = p.communicate()
        del(os.environ[ENV_PGPASSWORD])

        if stderr and len(stderr) > 0:
            raise SQLBackupError(stderr)

    @staticmethod
    def _get_today():
        return datetime.datetime.now().day

    def _do_backup(self, db_server, data):
        cfg = self._config
        user = data[SERVER_USER]
        passwd = data[SERVER_PASS]
        host = data[SERVER_HOST]
        port = data[SERVER_PORT]
        databases = data[SERVER_DATABASES]
        db_type = data[SERVER_TYPE]

        logger.info('Processing server \'{}\''.format(db_server))

        for database in databases:

            frequency = database[DB_FREQUENCY]
            db_name = database[DB_NAME]

            if frequency == Frequency.MONTHLY.value:
                pass
            elif frequency == Frequency.WEEKLY.value:
                pass
            elif frequency == Frequency.DAILY.value:
                pass
            else:
                # This is a wrong frequency value, log and continue.
                logger.error('{}[{}]: Backup not created, wrong value for parameter frequency: {}'
                             .format(db_server, db_name, frequency))
                continue

            verify = database[DB_VERIFY]
            compress = database[DB_COMPRESS]
            file_name = self.get_file_name(db_type, db_name)

            logger.info("Processing database '{}', type '{}'.".format(database, db_type))

            try:
                if db_type == DBType.MYSQL.value:
                    self.mysql_backup(user, passwd, db_name, file_name, host, port)
                else:
                    self.pgsql_backup(user, passwd, db_name, file_name, host, port)

                if compress:
                    compressed_file = self.compress(file_name)
                    if verify:
                        self.verify(compressed_file)

            except SQLBackupError as sql_e:
                logger.error('{}[{}]: {}'.format(db_server, db_name, sql_e))
                # Skip to next database
                continue

    def process(self):
        cfg = self._config
        db_servers = cfg[CFG_DB_SERVERS]
        for db_server in db_servers:
            data = db_servers[db_server]
            self._do_backup(db_server, data)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='SQL Backup Generator')
    parser.add_argument('config_file', action='store', help='Name of the config file')
    parser.add_argument('--logfile', action='store', help='Direct log messages to file (defaults to stdout)')
    parser.add_argument('--loglevel', action='store', help='Log message verbosity.')
    parser.add_argument('--logmode', action='store', choices=['append', 'overwrite'], help='Append or overwrite log file')
    args = parser.parse_args()

    if args.logfile:
        if args.logmode and args.logmode == 'overwrite':
            logging.basicConfig(filename=args.logfile, filemode='w')
        logging.basicConfig(filename=args.logfile)
    else:
        logging.basicConfig()
    if args.loglevel:
        logger.setLevel(args.loglevel)
    else:
        logger.setLevel(logging.INFO)

    sql_backup = SQLBackup()

    # Load config file
    sql_backup.load_config(args.config_file)

    # Process
    try:
        sql_backup.process()
    except Exception as e:
        exc_info = sys.exc_info()
        formatted = traceback.format_exc()
        logger.error(formatted)
        logger.error(e)
        exit(code=1)
