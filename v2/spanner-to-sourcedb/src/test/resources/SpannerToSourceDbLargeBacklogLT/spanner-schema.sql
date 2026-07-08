CREATE TABLE MigrationLoadTest (
  Id STRING(36) NOT NULL,
  Payload STRING(MAX) NOT NULL,
  migration_shard_id STRING(50),
) PRIMARY KEY (migration_shard_id, Id);

CREATE CHANGE STREAM MigrationStream
FOR MigrationLoadTest
OPTIONS (
  retention_period = '7d',
  value_capture_type = 'OLD_AND_NEW_VALUES'
);
