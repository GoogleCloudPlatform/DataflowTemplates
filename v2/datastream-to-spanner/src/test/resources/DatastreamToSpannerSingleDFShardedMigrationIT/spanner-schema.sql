CREATE TABLE IF NOT EXISTS Users (
    id INT64 NOT NULL,
    name STRING(200),
    age_spanner INT64,
    migration_shard_id STRING(50),
) PRIMARY KEY (migration_shard_id, id);