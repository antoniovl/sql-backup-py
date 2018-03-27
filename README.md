# sql-backup-py
**Tool for creating backups for MySQL and PostgreSQL - Python Version.**

This utility generates backup files from MySQL and PostgreSQL databases using their respective sql-dumping programs `mysqldump` and `pg_dump`.
Basic usage is:

`sqlbackup.py <config_file.json> --loglevel LEVEL --logfile LOGFILE --logmode MODE`

The JSON config file argument is mandatory, the others are optional. The optional arguments are:
* `--loglevel`: Sets the logging verbosity. Valid values are the ones from Python's logging module. Defaults to INFO.
* `--logfile`: Directs the log messages to a file rather than stdout.
* `--logmode`: This argument has 2 possible values:
  * `append`: Appends messages to the end of the logfile. This is the default.
  * `overwrite`: Reset the logfile erasing old messages.

The usage is to run the script from a cront task once a day. Depending on the config, it will generate daily, weekly or monthly backups:

* Daily indicates to generate a backup for that database. This is the default behavior.
* Weekly backups ocur once a week. If you say `'monday'` for `day_of_week` and if today is Monday then the backup for that database will be processed.
* Monthly backups ocur once a month, on the day you specify on `day_of_month`. I.e. if you specify `1` for `day_of_month` and today is the 1st of the month, then the backup for that database will be processed.

The generated backups are a SQL dump of a database, compressed if specified to. The file names are 'db_type-db_name-timestamp.sql.ext'. In example, for a database named `accounts` in a MySQL server and compressed with 7z, the resulting file name will be `mysql-accounts.sql.7z`. If you did not select to compress the files, the `7z, bz2` or `gz` extensions will be omitted.

This is a refresh from the original version written in Tcl. Is not been tested yet in Windows environments.

## The config file.
You need to provide a JSON file describing all the servers and databases that needs to be backed up. A sample file comes with the project, and the description of each setting is as follows.

* **`day_of_week:`** Specifies the day of week for weekly backups. It defaults to "sunday". 
* **`day_of_month:`** Day of the month when the montly backups will be generated. Be careful if you specify `31` here, because the backup won't be generated for some Months (i.e. February, April, June, etc).
* **`data_dir:`** Directory where the generated files will be stored. Defaults to `.` (curent dir).
* **`timestamps:`** If true then a timestamp 'YYYYMMDD-HHSS' will be appended to the backup's file name. This is useful for file rotation.
* **`mysql_dump_exe:`** Path to the `mysqldump` utility. Defaults to `/usr/bin/mysqldump`.
* **`pg_dump_exe:`** Path to the `pg_dump` utility. Defaults to `/usr/bin/pg_dump`.
* **`compression_type:`** Specifies the utility used to compress the backups. Valid values are `7z, bz2` and `gz`. I.e to compress with bzip2, put `bz2` in this value.
