CREATE TABLE Users (
    id INT64 NOT NULL,
    name STRING(200),
    age INT64,
    migration_shard_id STRING(50)
) PRIMARY KEY (migration_shard_id, id);
