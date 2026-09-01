CREATE TABLE Users (
  id INT64 NOT NULL,
  full_name STRING(255),
  location STRING(255),
) PRIMARY KEY(id);

CREATE TABLE Users2 (
  id INT64 NOT NULL,
  name STRING(255),
) PRIMARY KEY(id);

CREATE TABLE AllDatatypes (
  id INT64 NOT NULL,
  tinyint_col INT64,
  smallint_col INT64,
  int_col INT64,
  bigint_col INT64,
  bit_col BOOL,
  numeric_col NUMERIC,
  float_col FLOAT64,
  varchar_col STRING(255),
  date_col DATE,
  timestamp_col TIMESTAMP,
) PRIMARY KEY(id);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d',
  allow_txn_exclusion = true
);
