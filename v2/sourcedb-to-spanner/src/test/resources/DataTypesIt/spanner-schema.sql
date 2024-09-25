CREATE TABLE bigint_table (
  id INT64 NOT NULL,
  bigint_col INT64,
) PRIMARY KEY(id);

CREATE TABLE bigint_table (
  id INT64 NOT NULL,
  bigint_unsigned_col INT64,
) PRIMARY KEY(id);

CREATE TABLE binary_table (
  id INT64 NOT NULL,
  binary_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE bit_table (
  id INT64 NOT NULL,
  bit_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE blob_table (
  id INT64 NOT NULL,
  blob_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE bool_table (
  id INT64 NOT NULL,
  bool_col BOOL,
) PRIMARY KEY(id);

CREATE TABLE boolean_table (
  id INT64 NOT NULL,
  boolean_col BOOL,
) PRIMARY KEY(id);

CREATE TABLE char_table (
  id INT64 NOT NULL,
  char_col STRING(255),
) PRIMARY KEY(id);

CREATE TABLE date_table (
  id INT64 NOT NULL,
  date_col DATE,
) PRIMARY KEY(id);

CREATE TABLE datetime_table (
  id INT64 NOT NULL,
  datetime_col TIMESTAMP,
) PRIMARY KEY(id);

CREATE TABLE decimal_table (
  id INT64 NOT NULL,
  decimal_col NUMERIC,
) PRIMARY KEY(id);

CREATE TABLE double_table (
  id INT64 NOT NULL,
  double_col FLOAT64,
) PRIMARY KEY(id);

CREATE TABLE enum_table (
  id INT64 NOT NULL,
  enum_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE float_table (
  id INT64 NOT NULL,
  float_col FLOAT64,
) PRIMARY KEY(id);

CREATE TABLE int_table (
  id INT64 NOT NULL,
  int_col INT64,
) PRIMARY KEY(id);

CREATE TABLE `integer_unsigned_table` (
  id INT64 NOT NULL,
  int_col INT64,
) PRIMARY KEY(id);

CREATE TABLE json_table (
  id INT64 NOT NULL,
  json_col JSON,
) PRIMARY KEY(id);


CREATE TABLE longblob_table (
  id INT64 NOT NULL,
  longblob_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE longtext_table (
  id INT64 NOT NULL,
  longtext_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE mediumblob_table (
  id INT64 NOT NULL,
  mediumblob_col BYTES(MAX),
) PRIMARY KEY(id);

create table mediumint_table (
  id int64 not null,
  mediumint_col int64,
) primary key(id);

create table mediumint_unsigned_table (
  id int64 not null,
  mediumint_unsigned_col int64,
) primary key(id);


CREATE TABLE mediumtext_table (
  id INT64 NOT NULL,
  mediumtext_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE set_table (
  id INT64 NOT NULL,
  set_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE smallint_table (
  id INT64 NOT NULL,
  smallint_col INT64,
) PRIMARY KEY(id);

CREATE TABLE text_table (
  id INT64 NOT NULL,
  text_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE time_table (
  id INT64 NOT NULL,
  time_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE timestamp_table (
  id INT64 NOT NULL,
  timestamp_col TIMESTAMP,
) PRIMARY KEY(id);

CREATE TABLE tinyblob_table (
  id INT64 NOT NULL,
  tinyblob_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE tinyint_table (
  id INT64 NOT NULL,
  tinyint_col INT64,
) PRIMARY KEY(id);

CREATE TABLE tinyint_unsigned_table (
  id INT64 NOT NULL,
  tinyint_unsigned_col INT64,
) PRIMARY KEY(id);

CREATE TABLE tinytext_table (
  id INT64 NOT NULL,
  tinytext_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE varbinary_table (
  id INT64 NOT NULL,
  varbinary_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE varchar_table (
  id INT64 NOT NULL,
  varchar_col STRING(21000),
) PRIMARY KEY(id);

CREATE TABLE year_table (
  id INT64 NOT NULL,
  year_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE spatial_linestring (
  id INT64 NOT NULL,
  path STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE spatial_multilinestring (
  id INT64 NOT NULL,
  paths STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE spatial_multipoint (
  id INT64 NOT NULL,
  points STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE spatial_multipolygon (
  id INT64 NOT NULL,
  areas STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE spatial_point (
  id INT64 NOT NULL,
  location STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE spatial_polygon (
  id INT64 NOT NULL,
  area STRING(MAX),
) PRIMARY KEY(id);
CREATE TABLE `bigint_unsigned_pk_table` (
  id NUMERIC NOT NULL,
  bigint_unsigned_col NUMERIC NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE `string_pk_table` (
  id STRING(max) NOT NULL,
  string_col STRING(max) NOT NULL
) PRIMARY KEY(id);
