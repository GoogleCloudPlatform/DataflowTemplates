CREATE TABLE tinyint_table (id bigint PRIMARY KEY, tinyint_col smallint);
CREATE TABLE smallint_table (id bigint PRIMARY KEY, smallint_col smallint);
CREATE TABLE int_table (id bigint PRIMARY KEY, int_col integer);
CREATE TABLE bigint_table (id bigint PRIMARY KEY, bigint_col bigint);
CREATE TABLE bit_table (id bigint PRIMARY KEY, bit_col boolean);
CREATE TABLE decimal_table (id bigint PRIMARY KEY, decimal_col numeric);
CREATE TABLE numeric_table (id bigint PRIMARY KEY, numeric_col numeric);
CREATE TABLE money_table (id bigint PRIMARY KEY, money_col numeric);
CREATE TABLE smallmoney_table (id bigint PRIMARY KEY, smallmoney_col numeric);
CREATE TABLE float_table (id bigint PRIMARY KEY, float_col double precision);
CREATE TABLE real_table (id bigint PRIMARY KEY, real_col real);
CREATE TABLE date_table (id bigint PRIMARY KEY, date_col date);
CREATE TABLE time_table (id bigint PRIMARY KEY, time_col varchar);
CREATE TABLE datetime2_table (id bigint PRIMARY KEY, datetime2_col timestamp with time zone);
CREATE TABLE datetimeoffset_table (id bigint PRIMARY KEY, datetimeoffset_col timestamp with time zone);
CREATE TABLE datetime_table (id bigint PRIMARY KEY, datetime_col timestamp with time zone);
CREATE TABLE smalldatetime_table (id bigint PRIMARY KEY, smalldatetime_col timestamp with time zone);
CREATE TABLE char_table (id bigint PRIMARY KEY, char_col varchar);
CREATE TABLE varchar_table (id bigint PRIMARY KEY, varchar_col varchar);
CREATE TABLE text_table (id bigint PRIMARY KEY, text_col text);
CREATE TABLE nchar_table (id bigint PRIMARY KEY, nchar_col varchar);
CREATE TABLE nvarchar_table (id bigint PRIMARY KEY, nvarchar_col varchar);
CREATE TABLE ntext_table (id bigint PRIMARY KEY, ntext_col text);
CREATE TABLE binary_table (id bigint PRIMARY KEY, binary_col bytea);
CREATE TABLE varbinary_table (id bigint PRIMARY KEY, varbinary_col bytea);
CREATE TABLE image_table (id bigint PRIMARY KEY, image_col bytea);
CREATE TABLE uniqueidentifier_table (id bigint PRIMARY KEY, uniqueidentifier_col uuid);
CREATE TABLE xml_table (id bigint PRIMARY KEY, xml_col varchar);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d',
  allow_txn_exclusion = true
);
