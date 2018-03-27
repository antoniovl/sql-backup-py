#!/usr/bin/env python

from enum import Enum
from schematics.models import Model
from schematics.types import StringType, IntType, BooleanType, ModelType, ListType, DictType


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


class Database(Model):
    db_name = StringType(required=True)
    frequency = StringType(regex='daily|weekly|monthly', default='daily')
    compress = BooleanType(default=True)
    verify = BooleanType(default=True)


class DBServer(Model):
    user = StringType(required=True)
    password = StringType(required=True)
    hostname = StringType(required=True, default='localhost')
    port = IntType(required=True, max_value=65535, min_value=1)
    db_type = StringType(required=True, regex='mysql|postgresql')
    databases = ListType(ModelType(Database))


class SQLBackupConfig(Model):
    day_of_week = StringType(regex='(sunday|monday|tuesday|wednesday|thursday|friday|saturday)',
                             default='sunday')
    day_of_month = IntType(default=1, max_value=31, min_value=1)
    data_dir = StringType(default='.')
    timestamps = BooleanType(default=True)
    mysql_dump_exe = StringType(default='/usr/bin/mysqldump')
    pg_dump_exe = StringType(default='/usr/bin/pg_dump')
    compression_type = StringType(default=CompressionTypeEnum.P7Z.value)
    bzip2_exe = StringType(default='/usr/bin/bzip2')
    gzip_exe = StringType(default='/usr/bin/gzip')
    p7zip_exe = StringType(default='/usr/bin/7z')
    db_servers = DictType(ModelType(DBServer))
