#!/usr/bin/env python
import json
import jsonschema

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
_config_schema = {}


class Database(object):

    def __init__(self, data):
        self._db_name = data[DB_NAME]
        self._frequency = data[DB_FREQUENCY]
        self._compress = data[DB_COMPRESS]
        self._verify = data[DB_VERIFY]

    @property
    def db_name(self):
        return self._db_name

    @property
    def frequency(self):
        return self._frequency

    @property
    def compress(self):
        return self._compress

    @property
    def verify(self):
        return self._verify


class DBServer(object):

    def __init__(self, data):
        self._user = data[SERVER_USER]
        self._passwd = data[SERVER_PASS]
        self._host = data[SERVER_HOST]
        self._port = data[SERVER_PORT]
        self._db_type = data[SERVER_TYPE]
        self._set_databases(data[SERVER_DATABASES])

    def _set_databases(self, data):
        self._databases = []
        for item in data:
            database = Database(item)
            self.databases.append(database)

    @property
    def user(self):
        return self._user

    @property
    def passwd(self):
        return self._passwd

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def databases(self):
        return self._databases

    @property
    def db_type(self):
        return self._db_type


class SQLBackupConfig(object):

    def __init__(self, file_name):
        # Set the default config.
        self._set_config(DEFAULT_CONFIG)

        with open(file_name) as config_file:
            json_cfg = json.load(config_file)

        # Validate user provided config.
        self._validate(json_cfg, _config_schema)
        # Load if validation was ok
        self._set_config(json_cfg)

    def _set_config(self, json_cfg):
        """
        Copy config values from a dictionary.
        :param json_cfg:
        :return:
        """
        self._day_of_week = json_cfg[CFG_DAY_OF_WEEK]
        self._day_of_month = json_cfg[CFG_DAY_OF_MONTH]
        self._data_dir = json_cfg[CFG_DATA_DIR]
        self._timestamps = json_cfg[CFG_TIMESTAMPS]
        self._mysql_dump_exe = json_cfg[CFG_MYSQL_DUMP_EXE]
        self._pg_dump_exe = json_cfg[CFG_PG_DUMP_EXE]
        self._compression_type = json_cfg[CFG_COMPRESSION_TYPE]
        self._bzip2_exe = json_cfg[CFG_BZIP2_EXE]
        self._p7zip_exe = json_cfg[CFG_P7ZIP_EXE]
        self._gzip_exe = json_cfg[CFG_GZIP_EXE]
        self._set_db_servers(json_cfg[CFG_DB_SERVERS])

    def _set_db_servers(self, json_cfg):
        self._db_servers = dict()
        for server_key, value in json_cfg.items():
            db_server = DBServer(value)
            self._db_servers[server_key] = db_server

    def _validate(self, json_cfg, schema):
        pass

    @property
    def day_of_week(self):
        return self._day_of_week

    @day_of_week.setter
    def day_of_week(self, dow):
        pass

    @property
    def day_of_month(self):
        return self._day_of_month;

    @day_of_month.setter
    def day_of_month(self, dom):
        pass

    @property
    def data_dir(self):
        return self._data_dir

    @data_dir.setter
    def data_dir(self, dd):
        pass

    @property
    def timestamps(self):
        return self._timestamps

    @timestamps.setter
    def timestamps(self, ts):
        pass

    @property
    def mysql_dump_exe(self):
        return self._mysql_dump_exe

    @mysql_dump_exe.setter
    def mysql_dump_exe(self, mde):
        pass

    @property
    def pg_dump_exe(self):
        return self._pg_dump_exe

    @pg_dump_exe.setter
    def pg_dump_exe(self, pde):
        pass

    @property
    def compression_type(self):
        return self._compression_type

    @compression_type.setter
    def compression_type(self, ct):
        pass

    @property
    def bzip2_exe(self):
        return self._bzip2_exe

    @bzip2_exe.setter
    def bzip2_exe(self, bz):
        pass

    @property
    def p7zip_exe(self):
        return self._p7zip_exe

    @p7zip_exe.setter
    def p7zip_exe(self, p7z):
        pass

    @property
    def gzip_exe(self):
        return self._gzip_exe

    @gzip_exe.setter
    def gzip_exe(self, gz):
        pass

    @property
    def db_servers(self):
        return self._db_servers

    @db_servers.setter
    def db_servers(self, dbs):
        pass
