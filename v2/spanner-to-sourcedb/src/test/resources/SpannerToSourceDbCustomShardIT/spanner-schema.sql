CREATE TABLE IF NOT EXISTS Singers (
  SingerId INT64 NOT NULL,
  FirstName STRING(MAX),
) PRIMARY KEY(SingerId);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);