-- epoch for deleting series and label_key ids
CREATE TABLE SCHEMA_CATALOG.ids_epoch(
    current_epoch BIGINT NOT NULL
);

INSERT INTO SCHEMA_CATALOG.ids_epoch VALUES (0);

ALTER TABLE SCHEMA_CATALOG.series
    ADD COLUMN delete_epoch BIGINT;

ALTER TABLE SCHEMA_CATALOG.label_key
    ADD COLUMN delete_epoch BIGINT;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.epoch_abort()
RETURNS BOOLEAN AS $func$
$func$
BEGIN
    RAISE EXCEPTION 'epoch to old to continue INSERT'
END;
$func$ LANGUAGE PLPGSQL VOLATILE;
COMMENT ON PROCEDURE SCHEMA_CATALOG.epoch_abort()
IS 'ABORT an INSERT transaction due to the ID epoch being out of date';

-- Return a table name built from a full_name and a suffix.
-- The full name is truncated so that the suffix could fit in full.
-- name size will always be exactly 62 chars.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.pg_name_with_suffix(
        full_name text, suffix text)
    RETURNS name
AS $func$
    SELECT (substring(full_name for 62-(char_length(suffix)+1)) || '_' || suffix)::name
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.pg_name_with_suffix(text, text) TO prom_reader;
