CREATE TABLE IF NOT EXISTS Users (
    id INT64 NOT NULL,
    name STRING(200),
    age_spanner INT64,
    migration_shard_id STRING(50),
) PRIMARY KEY (migration_shard_id, id);

CREATE TABLE IF NOT EXISTS Movie (
    id1 INT64 NOT NULL,
    id2 INT64 NOT NULL,
    name STRING(200),
    actor INT64,
    migration_shard_id STRING(50),
) PRIMARY KEY (id2, id1, migration_shard_id);
