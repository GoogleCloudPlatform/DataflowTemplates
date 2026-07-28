CREATE TABLE Users (
  user_id INT64 NOT NULL,
  event_id STRING(MAX),
  full_name BYTES(MAX),
  age INT64,
  created_at TIMESTAMP,
  migration_shard_id STRING(MAX)
) PRIMARY KEY (migration_shard_id, user_id);
