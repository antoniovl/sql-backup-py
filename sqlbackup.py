#!/usr/bin/env python

import logging
import json
import argparse
import subprocess
import os
import traceback
import datetime
import time
import calendar

from schematics.exceptions import DataError
from common import SQLBackupConfig, SQLBackupError, DBType, CompressionTypeEnum, Frequency


ENV_MYSQL_PWD = 'MYSQL_PWD'
ENV_PGPASSWORD = 'PGPASSWORD'


logger = logging.getLogger(__name__)


class SQLBackup(object):

    def __init__(self, config_file):
        with open(config_file) as file:
            json_cfg = json.load(file)
        self.config = SQLBackupConfig(json_cfg)
        self.config.validate()

    @staticmethod
    def _get_timestamp(clock):
        return time.strftime('%Y%m%d-%H%M', clock)

    def get_file_name(self, db_type, db_name):
        """
        Calculates que backup file name using the db type prefix and the timestamp (if required).
        """
        cfg = self.config
        data_dir = cfg.data_dir

        if db_type == DBType.MYSQL.value:
            t = 'mysql'
        else:
            t = 'pgsql'

        if cfg.timestamps:
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
        cfg = self.config
        cmd = [cfg.bzip2_exe, '-9', file_name]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info('Compressing {}'.format(file_name))
        (stdout, stderr) = p.communicate()
        if stderr and len(stderr) > 0:
            raise SQLBackupError('Error compressing {}: {}'.format(file_name, stderr))

    def verify_bz2(self, file_name):
        """
        Verifies the bzip2 compressed file.
        """
        cfg = self.config
        cmd = [cfg.bzip2_exe, '-t', file_name]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info('Verifying {}'.format(file_name))
        (stdout, stderr) = p.communicate()
        if stderr and len(stderr) > 0:
            raise SQLBackupError('Integrity error in {}: {}'.format(file_name, stderr))

    def compress_gz(self, file_name):
        """
        See compress_bz2.
        :param file_name:
        :return:
        """
        cfg = self.config
        cmd = [cfg.gzip_exe, '-9', file_name]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info('Compressing {}'.format(file_name))
        (stdout, stderr) = p.communicate()
        if stderr and len(stderr) > 0:
            raise SQLBackupError('Error compressing {}: {}'.format(file_name, stderr))

    def verify_gz(self, file_name):
        cfg = self.config
        cmd = [cfg.gzip_exe, '-t', file_name]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info('Verifying {}'.format(file_name))
        (stdout, stderr) = p.communicate()
        if stderr and len(stderr) > 0:
            raise SQLBackupError('Integrity error in {}: {}'.format(file_name, stderr))

    def compress_7z(self, file_name):
        cfg = self.config
        p7z_file = '{}.7z'.format(file_name)
        cmd = [cfg.p7zip_exe, 'a', '-bd', '-t7z', '-m0=lzma', '-mx=9', '-mfb=64', '-md=64m', '-ms=on', '-sdel', p7z_file, file_name]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info('Compressing {}'.format(file_name))
        (stdout, stderr) = p.communicate()
        if stderr and len(stderr) > 0:
            raise SQLBackupError('Error compressing {}: {}'.format(file_name, stderr))

    def verify_7z(self, file_name):
        cfg = self.config
        cmd = [cfg.p7zip_exe, 't', file_name]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info('Verifying {}'.format(file_name))
        (stdout, stderr) = p.communicate()
        if stderr and len(stderr) > 0:
            raise SQLBackupError('Integrity error in {}: {}'.format(file_name, stderr))

    def compress(self, file_name):
        """
        Common routine that invokes the correct compression method based on the config values.
        :param file_name:
        :return:
        """
        cfg = self.config
        compression_type = cfg.compression_type
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
        cfg = self.config
        compression_type = cfg.compression_type
        if compression_type == CompressionTypeEnum.BZ2.value:
            self.verify_bz2(file_name)
        elif compression_type == CompressionTypeEnum.GZ.value:
            self.verify_gz(file_name)
        else:
            self.verify_7z(file_name)

    def mysql_backup(self, user, passwd, db, file_name, host='localhost', port=3306):
        """
        Generates a backup for MySQL.
        :param user: Database server username.
        :param passwd:
        :param db: Database name.
        :param file_name: Output file.
        :param host: Database host.
        :param port: Database port, defaults to 3306.
        :return:
        """
        cfg = self.config
        cmd = [
            cfg.mysql_dump_exe, '--opt', '--single-transaction',
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
        """
        Generates a backup for PostgreSQL.
        :param user: Database username.
        :param passwd:
        :param db: Database name.
        :param file_name: Output file.
        :param host: Database hostname.
        :param port: Database port, defaults to 5432.
        :return:
        """
        cfg = self.config
        cmd = [
            cfg.pg_dump_exe,
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

    @staticmethod
    def _get_day_of_week():
        wd = datetime.datetime.now().weekday()
        return calendar.day_name[wd].lower()

    def _do_backup(self, server_key, server_data):
        """
        Generates the backup.
        :param server_key: Key of the server in the config. Used as an identifier.
        :param server_data: Instance of DBServer. Configuration values.
        :return:
        """
        cfg = self.config
        user = server_data.user
        passwd = server_data.password
        host = server_data.hostname
        port = server_data.port
        databases = server_data.databases
        db_type = server_data.db_type

        logger.info('Processing server \'{}\''.format(server_key))

        for database in databases:

            frequency = database.frequency
            db_name = database.db_name

            if frequency == Frequency.MONTHLY.value:
                day_of_month = cfg.day_of_month
                today = self._get_today()
                if today != day_of_month:
                    logger.info("Skipping {} as it's scheduled for monthly backup at every {} and today is {}"
                                .format(db_name, today, day_of_month))
                    continue
            elif frequency == Frequency.WEEKLY.value:
                weekday = self._get_day_of_week()
                day_of_week = cfg.day_of_week
                if weekday != day_of_week:
                    logger.info("Skipping {} as it's scheduled for weekly backup at every {} and today is {}"
                                .format(db_name, day_of_week, weekday))
                    continue
            elif frequency == Frequency.DAILY.value:
                pass
            else:
                # This is a wrong frequency value, log and continue.
                logger.error('{}[{}]: Backup not created, wrong value for parameter frequency: {}'
                             .format(server_key, db_name, frequency))
                continue

            file_name = self.get_file_name(db_type, db_name)
            logger.info("Processing database '{}', type '{}', frequency {}."
                        .format(db_name, db_type, frequency))

            try:
                if db_type == DBType.MYSQL.value:
                    self.mysql_backup(user, passwd, db_name, file_name, host, port)
                else:
                    self.pgsql_backup(user, passwd, db_name, file_name, host, port)

                if database.compress:
                    compressed_file = self.compress(file_name)
                    if database.verify:
                        self.verify(compressed_file)

            except SQLBackupError as sql_e:
                logger.error('{}[{}]: {}'.format(server_key, db_name, sql_e))
                # Skip to next database
                continue

    def process(self):
        cfg = self.config
        db_servers = cfg.db_servers
        for db_server in db_servers:
            data = db_servers[db_server]
            self._do_backup(db_server, data)


def _exit(message, code=0, error=None, print_trace=False):
    print(message)
    if print_trace:
        format_exc = traceback.format_exc()
        print(format_exc)
    if error:
        print(error)
    exit(code)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='SQL Backup Generator')
    parser.add_argument('config_file', action='store', help='Name of the config file')
    parser.add_argument('--logfile', action='store', help='Direct log messages to file (defaults to stdout)')
    parser.add_argument('--loglevel', action='store', help='Log message verbosity.')
    parser.add_argument('--logmode', action='store', choices=['append', 'overwrite'], help='Append or overwrite log file')
    args = parser.parse_args()

    kwargs = dict()
    kwargs['format'] = '%(asctime)s:%(levelname)s: %(message)s'
    kwargs['datefmt'] = '%Y-%m-%d %H:%M:%S'
    if args.logfile:
        kwargs['filename'] = args.logfile
        if args.logmode and args.logmode == 'overwrite':
            kwargs['filemode'] = 'w'
    logging.basicConfig(**kwargs)

    if args.loglevel:
        try:
            logger.setLevel(args.loglevel)
        except ValueError as ve:
            _exit('\nCritical error processing loglevel argument:\n', code=1, error=ve)
    else:
        logger.setLevel(logging.INFO)

    # Process
    try:
        sql_backup = SQLBackup(args.config_file)
        logger.info('****************************')
        logger.info('Starting sql-backup process.')
        sql_backup.process()
    except DataError as de:
        _exit("Configuration Error Found - Can't Continue. ", code=1, error=de)
    except Exception as e:
        _exit('Critical Error found - Can\'t Continue. ', code=1, error=e)

