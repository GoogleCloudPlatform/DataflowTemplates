CREATE TABLE AccountRoles (
    migration_shard_id STRING(50),
    role_id INT64 NOT NULL,
    role_name STRING(255)
) PRIMARY KEY (migration_shard_id, role_id);

CREATE TABLE Users (
    migration_shard_id STRING(50),
    user_id INT64 NOT NULL,
    event_id STRING(255) NOT NULL,
    full_name STRING(255),
    age INT64,
    created_at TIMESTAMP
) PRIMARY KEY (migration_shard_id, user_id, event_id);
