CREATE TABLE bool_table (id INT64, bool_col BOOL) PRIMARY KEY(id);
CREATE TABLE int64_table (id INT64, int64_col INT64) PRIMARY KEY(id);
CREATE TABLE float64_table (id INT64, float64_col FLOAT64) PRIMARY KEY(id);
CREATE TABLE string_table (id INT64, string_col STRING(MAX)) PRIMARY KEY(id);
CREATE TABLE bytes_table (id INT64, bytes_col BYTES(MAX)) PRIMARY KEY(id);
CREATE TABLE date_table (id INT64, date_col DATE) PRIMARY KEY(id);
CREATE TABLE numeric_table (id INT64, numeric_col NUMERIC) PRIMARY KEY(id);
CREATE TABLE timestamp_table (id INT64, timestamp_col TIMESTAMP) PRIMARY KEY(id);
CREATE TABLE json_table (id INT64, json_col JSON) PRIMARY KEY(id);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d',
  allow_txn_exclusion = true
);