CREATE TABLE `person` (
    `first_name1` STRING(500),
    `last_name1` STRING(500),
    `id` STRING(100) NOT NULL,
)  PRIMARY KEY(id);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);