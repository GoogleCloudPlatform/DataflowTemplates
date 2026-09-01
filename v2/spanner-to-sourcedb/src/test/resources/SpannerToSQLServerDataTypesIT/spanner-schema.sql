CREATE TABLE tinyint_table (id INT64, tinyint_col INT64) PRIMARY KEY(id);
CREATE TABLE smallint_table (id INT64, smallint_col INT64) PRIMARY KEY(id);
CREATE TABLE int_table (id INT64, int_col INT64) PRIMARY KEY(id);
CREATE TABLE bigint_table (id INT64, bigint_col INT64) PRIMARY KEY(id);
CREATE TABLE bit_table (id INT64, bit_col BOOL) PRIMARY KEY(id);
CREATE TABLE decimal_table (id INT64, decimal_col NUMERIC) PRIMARY KEY(id);
CREATE TABLE numeric_table (id INT64, numeric_col NUMERIC) PRIMARY KEY(id);
CREATE TABLE money_table (id INT64, money_col NUMERIC) PRIMARY KEY(id);
CREATE TABLE smallmoney_table (id INT64, smallmoney_col NUMERIC) PRIMARY KEY(id);
CREATE TABLE float_table (id INT64, float_col FLOAT64) PRIMARY KEY(id);
CREATE TABLE real_table (id INT64, real_col FLOAT32) PRIMARY KEY(id);
CREATE TABLE date_table (id INT64, date_col DATE) PRIMARY KEY(id);
CREATE TABLE time_table (id INT64, time_col STRING(MAX)) PRIMARY KEY(id);
CREATE TABLE datetime2_table (id INT64, datetime2_col TIMESTAMP) PRIMARY KEY(id);
CREATE TABLE datetimeoffset_table (id INT64, datetimeoffset_col TIMESTAMP) PRIMARY KEY(id);
CREATE TABLE datetime_table (id INT64, datetime_col TIMESTAMP) PRIMARY KEY(id);
CREATE TABLE smalldatetime_table (id INT64, smalldatetime_col TIMESTAMP) PRIMARY KEY(id);
CREATE TABLE char_table (id INT64, char_col STRING(MAX)) PRIMARY KEY(id);
CREATE TABLE varchar_table (id INT64, varchar_col STRING(MAX)) PRIMARY KEY(id);
CREATE TABLE text_table (id INT64, text_col STRING(MAX)) PRIMARY KEY(id);
CREATE TABLE nchar_table (id INT64, nchar_col STRING(MAX)) PRIMARY KEY(id);
CREATE TABLE nvarchar_table (id INT64, nvarchar_col STRING(MAX)) PRIMARY KEY(id);
CREATE TABLE ntext_table (id INT64, ntext_col STRING(MAX)) PRIMARY KEY(id);
CREATE TABLE binary_table (id INT64, binary_col BYTES(MAX)) PRIMARY KEY(id);
CREATE TABLE varbinary_table (id INT64, varbinary_col BYTES(MAX)) PRIMARY KEY(id);
CREATE TABLE image_table (id INT64, image_col BYTES(MAX)) PRIMARY KEY(id);
CREATE TABLE uniqueidentifier_table (id INT64, uniqueidentifier_col STRING(MAX)) PRIMARY KEY(id);
CREATE TABLE xml_table (id INT64, xml_col STRING(MAX)) PRIMARY KEY(id);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d',
  allow_txn_exclusion = true
);
