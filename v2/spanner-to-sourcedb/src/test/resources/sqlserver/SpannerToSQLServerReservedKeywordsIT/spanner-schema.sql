CREATE TABLE IF NOT EXISTS `true` (
  `COLUMN` INT64 NOT NULL,
  `TABLE` STRING(255),
  `WITH` STRING(255)
) PRIMARY KEY (`COLUMN`);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d',
  allow_txn_exclusion = true
);
