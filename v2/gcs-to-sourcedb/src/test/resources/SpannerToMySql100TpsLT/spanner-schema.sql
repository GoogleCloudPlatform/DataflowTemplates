CREATE TABLE IF NOT EXISTS `Person` (
    `first_name1` STRING(500),
    `last_name1` STRING(500),
    `first_name2` STRING(500),
    `last_name2` STRING(500),
    `first_name3` STRING(500),
    `last_name3` STRING(500),
    `ID` STRING(100) NOT NULL,
)  PRIMARY KEY(ID);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);
