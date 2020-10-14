-- epoch for deleting series and label_key ids
CREATE TABLE SCHEMA_CATALOG.ids_epoch(
    current_epoch BIGINT NOT NULL,
    last_update_time TIMESTAMPTZ NOT NULL,
);

INSERT INTO SCHEMA_CATALOG.ids_epoch VALUES (0, now());

ALTER TABLE SCHEMA_CATALOG.series
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

--drop chunks from metrics tables and delete the appropriate series.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.drop_metric_chunks(metric_name TEXT, older_than TIMESTAMPTZ)
    RETURNS BOOLEAN
    AS $func$
DECLARE
    metric_table NAME;
    check_time TIMESTAMPTZ;
    time_dimension_id INT;
    label_array int[];
    last_epoch_time TIMESTAMPTZ;
    deletion_epoch BIGINT;
BEGIN
    SELECT table_name
    INTO STRICT metric_table
    FROM SCHEMA_CATALOG.get_or_create_metric_table_name(metric_name);

    SELECT older_than + INTERVAL '1 hour'
    INTO check_time;

    -- transaction 1
    BEGIN;
        IF SCHEMA_CATALOG.is_timescaledb_installed() THEN
            --Get the time dimension id for the time dimension
            SELECT d.id
            INTO STRICT time_dimension_id
            FROM _timescaledb_catalog.hypertable h
            INNER JOIN _timescaledb_catalog.dimension d ON (d.hypertable_id = h.id)
            WHERE h.schema_name = 'SCHEMA_DATA' AND h.table_name = metric_table
            ORDER BY d.id ASC
            LIMIT 1;

            --Get a tight older_than (EXCLUSIVE) because we want to know the
            --exact cut-off where things will be dropped
            SELECT _timescaledb_internal.to_timestamp(range_end)
            INTO older_than
            FROM _timescaledb_catalog.chunk c
            INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (c.id = cc.chunk_id)
            INNER JOIN _timescaledb_catalog.dimension_slice ds ON (ds.id = cc.dimension_slice_id)
            --range_end is exclusive so this is everything < older_than (which is also exclusive)
            WHERE ds.dimension_id = time_dimension_id AND ds.range_end <= _timescaledb_internal.to_unix_microseconds(older_than)
            ORDER BY range_end DESC
            LIMIT 1;
        END IF;

        IF older_than IS NULL THEN
            RETURN false;
        END IF;

        --chances are that the hour after the drop point will have the most similar
        --series to what is dropped, so first filter by all series that have been dropped
        --but that aren't in that first hour and then make sure they aren't in the dataset
        EXECUTE format(
        $query$
            WITH potentially_drop_series AS (
                SELECT distinct series_id
                FROM SCHEMA_DATA.%1$I
                WHERE time < %2$L
                EXCEPT
                SELECT distinct series_id
                FROM SCHEMA_DATA.%1$I
                WHERE time >= %2$L AND time < %3$L
            ), confirmed_drop_series AS (
                SELECT series_id
                FROM potentially_drop_series
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM  SCHEMA_DATA.%1$I  data_exists
                    WHERE data_exists.series_id = potentially_drop_series.series_id AND time >= %3$L
                    --use chunk append + more likely to find something starting at earliest time
                    ORDER BY time ASC
                    LIMIT 1
                )
            )
            UPDATE SCHEMA_DATA_SERIES.%1$I SET delete_epoch = current_epoch+1
            FROM SCHEMA_DATA_SERIES.ids_epoch
            WHERE delete_epoch IS NULL
        $query$, metric_table, older_than, check_time);
    COMMIT;

    -- transaction 2
    BEGIN;
        SELECT last_update_time, current_epoch FROM SCHEMA_CATALOG.ids_epoch LIMIT 1
            INTO last_epoch_time, deletion_epoch;
        IF last_epoch_time + '1 hour' < now() THEN
                RETURN true; -- TODO
        END IF;

        UPDATE SCHEMA_CATALOG.ids_epoch
            SET (current_epoch, last_update_time) = (current_epoch + 1, now());

        EXECUTE format($query$
            WITH dead_series AS (
                SELECT id FROM SCHEMA_DATA_SERIES.%1$I WHERE NOT EXISTS (SELECT 1 FROM SCHEMA_DATA.%1$I WHERE id = label_id LIMIT 1)
            ), deleted_series AS (
                DELETE FROM SCHEMA_DATA_SERIES.%1$I
                WHERE delete_epoch <= deletion_epoch AND id IN dead_series
                RETURNING id, labels
            ), resurrected_series AS (
                UPDATE SCHEMA_DATA_SERIES.%1$I
                SET delete_epoch TO NULL
                WHERE delete_epoch <= deletion_epoch
            )
            SELECT ARRAY(SELECT DISTINCT unnest(labels) as label_id
            FROM deleted_series
        $query$, metric_table) INTO label_array;

        --needs to be a separate query and not a CTE since this needs to "see"
        --the series rows deleted above as deleted.
        --Note: we never delete metric name keys since there are check constraints that
        --rely on those ids not changing.
        EXECUTE format($query$
        WITH confirmed_drop_labels AS (
                SELECT label_id
                FROM unnest($1) as labels(label_id)
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM  SCHEMA_DATA_SERIES.%1$I series_exists
                    WHERE series_exists.labels && ARRAY[labels.label_id]
                    LIMIT 1
                )
            )
            DELETE FROM SCHEMA_CATALOG.label
            WHERE id IN (SELECT * FROM confirmed_drop_labels) AND key != '__name__';
        $query$, metric_table) USING label_array;

        IF SCHEMA_CATALOG.is_timescaledb_installed() THEN
                PERFORM drop_chunks(table_name=>metric_table, schema_name=> 'SCHEMA_DATA', older_than=>older_than, cascade_to_materializations=>FALSE);
        ELSE
                EXECUTE format($$ DELETE FROM SCHEMA_DATA.%I WHERE time < %L $$, metric_table, older_than);
        END IF;
   COMMIT;

   RETURN true;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
