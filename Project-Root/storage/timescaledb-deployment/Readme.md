


### Command to check all available installed extentions in the Postgres

SELECT *
FROM pg_available_extensions 
WHERE name = 'timescaledb';


SELECT default_version, installed_version 
FROM pg_available_extensions 
WHERE name = 'timescaledb';


SELECT extname, extversion FROM pg_extension WHERE extname = 'timescaledb';


### Activate a particular extension which is installed

CREATE EXTENSION IF NOT EXISTS timescaledb;


### Get list of all hypertables

SELECT 
    hypertable_schema, 
    hypertable_name, 
    num_chunks, 
    compression_enabled 
FROM timescaledb_information.hypertables;


