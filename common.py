#!/usr/bin/env python
import json

from enum import Enum
from schematics.models import Model
from schematics.types import StringType, IntType, BooleanType, ModelType, ListType, DictType

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

#DAYS_OF_WEEK = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']
#LOG_MODES = ['append', 'overwrite']


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


DEFAULT_CONFIG = {
    CFG_DAY_OF_WEEK: 'sunday',
    CFG_DAY_OF_MONTH: 1,
    CFG_DATA_DIR: 'baks',
    CFG_COMPRESSION_TYPE: 'bz2',
    CFG_TIMESTAMPS: True,
    CFG_MYSQL_DUMP_EXE: '/opt/local/bin/mysqldump',
    CFG_BZIP2_EXE: '/usr/bin/bzip2',
    CFG_P7ZIP_EXE: '/usr/bin/7z',
    CFG_GZIP_EXE: '/usr/bin/gzip',
    CFG_PG_DUMP_EXE: '/opt/local/bin/pg_dump',
    CFG_DB_SERVERS: {}
}


class Database(Model):
    db_name = StringType(required=True)
    frequency = StringType(required=True, regex='daily|weekly|monthly', default='daily')
    compress = BooleanType(required=True, default=True)
    verify = BooleanType(required=True, default=True)


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
