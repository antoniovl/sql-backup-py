# sql-backup-py
**Tool for creating backups for MySQL and PostgreSQL - Python Version.**

This utility generates backup files from MySQL and PostgreSQL databases using their respective sql-dumping programs `mysqldump` and `pg_dump`.
Basic usage is:

`sqlbackup.py <config_file.json> --loglevel LEVEL --logfile LOGFILE --logmode MODE`
The JSON config file argument is mandatory, the others are optional. The optional arguments are:
* --loglevel: Sets the logging verbosity. Valid values are the ones from Python's logging module. Defaults to INFO.
* --logfile: Directs the log messages to a file rather than stdout.
* --logmode: This argument has 2 possible values:
  * append: Appends messages to the end of the logfile. This is the default.
	* overwrite: Reset the logfile erasing old messages.
	
## The config file.
