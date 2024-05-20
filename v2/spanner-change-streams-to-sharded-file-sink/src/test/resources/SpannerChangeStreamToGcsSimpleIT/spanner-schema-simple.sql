CREATE TABLE Singers (
  SingerId INT64 NOT NULL,
  FirstName STRING(MAX),
  LastName STRING(MAX),
  shardId STRING(20),
  update_ts TIMESTAMP,
  migration_shard_id STRING(50),
) PRIMARY KEY(SingerId, migration_shard_id);


CREATE TABLE sample_table (
  id INT64 NOT NULL,
  varchar_column STRING(20),
  tinyint_column INT64,
  text_column STRING(MAX),
  date_column DATE,
  smallint_column INT64,
  mediumint_column INT64,
  bigint_column INT64,
  float_column FLOAT64,
  double_column FLOAT64,
  decimal_column NUMERIC,
  datetime_column TIMESTAMP,
  timestamp_column TIMESTAMP,
  time_column STRING(MAX),
  year_column STRING(MAX),
  char_column STRING(10),
  tinyblob_column BYTES(MAX),
  tinytext_column STRING(MAX),
  blob_column BYTES(MAX),
  mediumblob_column BYTES(MAX),
  mediumtext_column STRING(MAX),
  longblob_column BYTES(MAX),
  longtext_column STRING(MAX),
  enum_column STRING(MAX),
  bool_column BOOL,
  binary_column BYTES(MAX),
  varbinary_column BYTES(MAX),
  update_ts TIMESTAMP,
  migration_shard_id STRING(50),
) PRIMARY KEY(id, migration_shard_id);



CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);