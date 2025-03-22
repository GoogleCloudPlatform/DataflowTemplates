DROP TABLE IF EXISTS customers;
CREATE TABLE IF NOT EXISTS customers (
    id INT64 NOT NULL,
    first_name STRING(25),
    last_name STRING(25),
    empty_string STRING(25),
    null_key STRING(25)
) PRIMARY KEY(id);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);
