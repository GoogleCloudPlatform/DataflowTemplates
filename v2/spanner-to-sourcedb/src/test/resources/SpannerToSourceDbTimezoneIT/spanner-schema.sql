ALTER DATABASE db SET OPTIONS (default_time_zone = 'Australia/Brisbane');

CREATE TABLE IF NOT EXISTS Users (
    id INT64 NOT NULL,
    time_colm TIMESTAMP
) PRIMARY KEY(id);


CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d',
  allow_txn_exclusion = true
);