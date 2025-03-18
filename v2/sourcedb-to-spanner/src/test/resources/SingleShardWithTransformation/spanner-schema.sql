CREATE TABLE IF NOT EXISTS SingleShardWithTransformationTable (
	pkid INT64 NOT NULL,
	name STRING(20),
	status STRING(20),
	migration_shard_id STRING(50),
) PRIMARY KEY (migration_shard_id, pkid);