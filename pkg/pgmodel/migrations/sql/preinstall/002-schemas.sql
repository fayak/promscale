
CREATE SCHEMA IF NOT EXISTS SCHEMA_CATALOG; -- catalog tables + internal functions
GRANT USAGE ON SCHEMA SCHEMA_CATALOG TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_CATALOG TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_CATALOG GRANT SELECT ON TABLES TO prom_reader;
GRANT USAGE ON SCHEMA SCHEMA_CATALOG TO prom_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_CATALOG TO prom_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_CATALOG GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO prom_writer;

CREATE SCHEMA IF NOT EXISTS SCHEMA_PROM; -- public functions
GRANT USAGE ON SCHEMA SCHEMA_PROM TO prom_reader;

CREATE SCHEMA IF NOT EXISTS SCHEMA_EXT; -- optimized versions of functions created by the extension
GRANT USAGE ON SCHEMA SCHEMA_EXT TO prom_reader;

CREATE SCHEMA IF NOT EXISTS SCHEMA_SERIES; -- series views
GRANT USAGE ON SCHEMA SCHEMA_SERIES TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_SERIES TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_SERIES GRANT SELECT ON TABLES TO prom_reader;

CREATE SCHEMA IF NOT EXISTS SCHEMA_METRIC; -- metric views
GRANT USAGE ON SCHEMA SCHEMA_METRIC TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_METRIC TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_METRIC GRANT SELECT ON TABLES TO prom_reader;

CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA;
GRANT USAGE ON SCHEMA SCHEMA_DATA TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_DATA TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA GRANT SELECT ON TABLES TO prom_reader;
GRANT USAGE ON SCHEMA SCHEMA_DATA TO prom_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_DATA TO prom_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO prom_writer;

CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA_SERIES;
GRANT USAGE ON SCHEMA SCHEMA_DATA_SERIES TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_DATA_SERIES TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA_SERIES GRANT SELECT ON TABLES TO prom_reader;
GRANT USAGE ON SCHEMA SCHEMA_DATA_SERIES TO prom_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_DATA_SERIES TO prom_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA_SERIES GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO prom_writer;


CREATE SCHEMA IF NOT EXISTS SCHEMA_INFO;
GRANT USAGE ON SCHEMA SCHEMA_INFO TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_INFO TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_INFO GRANT SELECT ON TABLES TO prom_reader;


-- the promscale extension contains optimized version of some
-- of our functions and operators. To ensure the correct version of the are
-- used, SCHEMA_EXT must be before all of our other schemas in the search path
DO $$
DECLARE
   new_path text;
BEGIN
   new_path := current_setting('search_path') || format(',%L,%L,%L,%L', 'SCHEMA_EXT', 'SCHEMA_PROM', 'SCHEMA_METRIC', 'SCHEMA_CATALOG');
   execute format('ALTER DATABASE %I SET search_path = %s', current_database(), new_path);
   execute format('SET search_path = %s', new_path);
END
$$;
