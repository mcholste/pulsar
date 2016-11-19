# pulsar
Event collection, parsing, normalization, archiving, and indexing into ElasticSearch

# Event Collection
Pulsar is a simple Python script which reads JSON from stdin and writes to multiple destinations. By default, it writes to a local archive and to an ElasticSearch destination. Pulsar is designed to be deployed as a Syslog-NG program destnation, but it can read arbitrary JSON from stdin. It contains a Syslog-NG pattern database for parsing logs into structured JSON which is then passed to Pulsar.

The basic execution flow is:
```
raw logs --> Syslog-NG (parses to JSON) --> Pulsar stdin --> decoration (geoip, timestamps) --> ES/archive
```

All phases of execution happen in separate processes with basic Python queues used to pass data back and forth.

# Decoration
Pulsar's decorator function consists primarily of timestamp handling and some specific field handling for Syslog-NG and [Galaxy](https://github.com/mcholste/galaxy). It has optional support for MaxMind's GeoIP library and database. The included Docker container will build and install this functionality.

# Archive
The built-in archiver writes json.gz file to date-templated directories. Files roll at 10MB or 10,000 events (configurable). A simple sqlite database is kept to provide a directory and inventory of available archive files.

# ElasticSearch
Pulsar uses the official ElasticSearch Python libraries to write bulk entries to ES.

# Configuration
The only command line argument available is the file location of an optional config file. If none is provided, defaults will be used. Here is an example config file:
```
{
  "log_level": "DEBUG",
  "log_file": "/var/log/pulsar.log",
  "host": "localhost", # ElasticSearch host
  "port": 9200, # ES port
  "directory": "/var/pulsar", # Directory for database and archive files
  "batch_limit": 10000, # Max events to write to a single archive json.gz
  "batch_size_limit": 10000000, # Max size of a single archive file
  "geoip_db": "/usr/local/share/GeoIP/GeoLite2-City.mmdb" # Location of GeoIP DB
}
```

# Docker
Pulsar is available on Docker Hub and the container includes a fully configured Syslog-NG as well as the Pulsar pattern database for parsing log files.
```
docker pull mcholste/pulsar && docker run pulsar
```

