CREATE TABLE Singers (
SingerId INT64 NOT NULL,
FirstName STRING(MAX),
LastName STRING(MAX),
shardId STRING(20),
update_ts TIMESTAMP,
hb_shardId STRING(20),
) PRIMARY KEY(SingerId);


CREATE CHANGE STREAM allstream
FOR ALL OPTIONS (
retention_period = '7d',
value_capture_type = 'NEW_ROW'
);