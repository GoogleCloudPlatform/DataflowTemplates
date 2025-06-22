CREATE TABLE IF NOT EXISTS bigint_table (
  id INT64 NOT NULL,
  bigint_col int64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS bigint_unsigned_table (
  id INT64 NOT NULL,
  bigint_unsigned_col NUMERIC,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS binary_table (
  id INT64 NOT NULL,
  binary_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS bit_table (
  id INT64 NOT NULL,
  bit_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS blob_table (
  id INT64 NOT NULL,
  blob_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS bool_table (
  id INT64 NOT NULL,
  bool_col BOOL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS boolean_table (
  id INT64 NOT NULL,
  boolean_col BOOL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS char_table (
  id INT64 NOT NULL,
  char_col STRING(255),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS date_table (
  id INT64 NOT NULL,
  date_col DATE,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS datetime_table (
  id INT64 NOT NULL,
  datetime_col TIMESTAMP,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS decimal_table (
  id INT64 NOT NULL,
  decimal_col NUMERIC,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS double_table (
  id INT64 NOT NULL,
  double_col FLOAT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS enum_table (
  id INT64 NOT NULL,
  enum_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS float_table (
  id INT64 NOT NULL,
  float_col FLOAT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS int_table (
  id INT64 NOT NULL,
  int_col INT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS `integer_unsigned_table` (
  id INT64 NOT NULL,
  integer_unsigned_col INT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS `smallint_unsigned_table` (
  id INT64 NOT NULL,
  `smallint_unsigned_col` INT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS test_json_table (
  id INT64 NOT NULL,
  test_json_col JSON,
) PRIMARY KEY(id);


CREATE TABLE IF NOT EXISTS longblob_table (
  id INT64 NOT NULL,
  longblob_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS longtext_table (
  id INT64 NOT NULL,
  longtext_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS mediumblob_table (
  id INT64 NOT NULL,
  mediumblob_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS mediumint_table (
  id int64 not null,
  mediumint_col int64,
) primary key(id);

CREATE TABLE IF NOT EXISTS mediumint_unsigned_table (
  id int64 not null,
  mediumint_unsigned_col int64,
) primary key(id);


CREATE TABLE IF NOT EXISTS mediumtext_table (
  id INT64 NOT NULL,
  mediumtext_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS set_table (
  id INT64 NOT NULL,
  set_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS smallint_table (
  id INT64 NOT NULL,
  smallint_col INT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS text_table (
  id INT64 NOT NULL,
  text_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS time_table (
  id INT64 NOT NULL,
  time_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS timestamp_table (
  id INT64 NOT NULL,
  timestamp_col TIMESTAMP,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS tinyblob_table (
  id INT64 NOT NULL,
  tinyblob_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS tinyint_table (
  id INT64 NOT NULL,
  tinyint_col INT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS tinyint_unsigned_table (
  id INT64 NOT NULL,
  tinyint_unsigned_col INT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS tinytext_table (
  id INT64 NOT NULL,
  tinytext_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS varbinary_table (
  id INT64 NOT NULL,
  varbinary_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS varchar_table (
  id INT64 NOT NULL,
  varchar_col STRING(21000),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS year_table (
  id INT64 NOT NULL,
  year_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS spatial_linestring (
  id INT64 NOT NULL,
  path STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS spatial_multilinestring (
  id INT64 NOT NULL,
  paths STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS spatial_multipoint (
  id INT64 NOT NULL,
  points STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS spatial_multipolygon (
  id INT64 NOT NULL,
  areas STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS spatial_point (
  id INT64 NOT NULL,
  location STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS spatial_polygon (
  id INT64 NOT NULL,
  area STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS bigint_pk_table (
  id INT64 NOT NULL,
  bigint_pk_col INT64 NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS bigint_unsigned_pk_table (
  id NUMERIC NOT NULL,
  bigint_unsigned_pk_col NUMERIC NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS int_pk_table (
   id INT64 NOT NULL,
   int_pk_col INT64 NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS int_unsigned_pk_table (
   id INT64 NOT NULL,
   int_unsigned_pk_col INT64 NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS medium_int_pk_table (
   id INT64 NOT NULL,
   medium_int_pk_col INT64 NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS medium_int_unsigned_pk_table (
   id INT64 NOT NULL,
   medium_int_unsigned_pk_col INT64 NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS small_int_pk_table (
   id INT64 NOT NULL,
   small_int_pk_col INT64 NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS small_int_unsigned_pk_table (
   id INT64 NOT NULL,
   small_int_unsigned_pk_col INT64 NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS tiny_int_pk_table (
   id INT64 NOT NULL,
   tiny_int_pk_col INT64 NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS tiny_int_unsigned_pk_table (
   id INT64 NOT NULL,
   tiny_int_unsigned_pk_col INT64 NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS binary_pk_table (
  id BYTES(20) NOT NULL,
  binary_pk_col BYTES(20) NOT NULL,
) PRIMARY KEY(id);


CREATE TABLE IF NOT EXISTS varbinary_pk_table (
  id BYTES(20) NOT NULL,
  varbinary_pk_col BYTES(20) NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS tiny_blob_pk_table (
  id BYTES(20) NOT NULL,
  tiny_blob_pk_col BYTES(20) NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS char_pk_table (
  id STRING(20) NOT NULL,
  char_pk_col STRING(20) NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS varchar_pk_table (
  id STRING(20) NOT NULL,
  varchar_pk_col STRING(20) NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS tiny_text_pk_table (
  id STRING(20) NOT NULL,
  tiny_text_pk_col STRING(20) NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS date_time_pk_table (
    id TIMESTAMP NOT NULL,
    date_time_pk_col TIMESTAMP NOT NULL,
    ) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS timestamp_pk_table (
                                                  id TIMESTAMP NOT NULL,
                                                  timestamp_pk_col TIMESTAMP NOT NULL,
) PRIMARY KEY(id);
