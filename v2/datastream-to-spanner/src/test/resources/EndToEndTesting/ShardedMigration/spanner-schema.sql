CREATE TABLE Authors (
    id INT64 NOT NULL ,
    name STRING(200),
    migration_shard_id STRING(50),
) PRIMARY KEY (migration_shard_id, id);

CREATE CHANGE STREAM allstream
    FOR ALL OPTIONS (
    value_capture_type = 'NEW_ROW',
    retention_period = '7d'
);
