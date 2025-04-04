CREATE TABLE large_data (
    id STRING(36) NOT NULL,
    large_blob BYTES(10485760) NOT NULL
) PRIMARY KEY (id);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);
