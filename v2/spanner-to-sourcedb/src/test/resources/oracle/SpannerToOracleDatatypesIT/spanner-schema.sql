CREATE TABLE default_types_table (
  id STRING(MAX) NOT NULL,
  varchar2_col STRING(MAX),
  varchar_col STRING(MAX),
  char_col STRING(MAX),
  character_col STRING(MAX),
  nvarchar2_col STRING(MAX),
  nchar_col STRING(MAX),
  nchar_varying_col STRING(MAX),
  national_character_col STRING(MAX),
  national_char_col STRING(MAX),
  national_character_varying_col STRING(MAX),
  national_char_varying_col STRING(MAX),
  number_col NUMERIC,
  numeric_col NUMERIC,
  decimal_col NUMERIC,
  dec_col NUMERIC,
  float_col NUMERIC,
  double_precision_col FLOAT64,
  real_col FLOAT64,
  binary_float_col FLOAT32,
  binary_double_col FLOAT64,
  integer_col INT64,
  int_col INT64,
  smallint_col INT64,
  date_col TIMESTAMP,
  timestamp_col TIMESTAMP,
  timestamp_with_time_zone_col TIMESTAMP,
  timestamp_with_local_time_zone_col TIMESTAMP,
  raw_col BYTES(MAX),
  blob_col BYTES(MAX),
  clob_col STRING(MAX),
  nclob_col STRING(MAX),
  boolean_col BOOL,
  json_col JSON
) PRIMARY KEY(id);

CREATE TABLE alt_types_table (
  id STRING(MAX) NOT NULL,
  varchar2_to_string_col STRING(MAX),
  varchar_to_string_col STRING(MAX),
  char_to_string_col STRING(MAX),
  character_to_string_col STRING(MAX),
  nvarchar2_to_string_col STRING(MAX),
  nchar_to_string_col STRING(MAX),
  nchar_varying_to_string_col STRING(MAX),
  national_character_to_string_col STRING(MAX),
  national_char_to_string_col STRING(MAX),
  national_character_varying_to_string_col STRING(MAX),
  national_char_varying_to_string_col STRING(MAX),
  number_to_float64_col FLOAT64,
  numeric_to_float64_col FLOAT64,
  decimal_to_float64_col FLOAT64,
  dec_to_float64_col FLOAT64,
  float_to_float64_col FLOAT64,
  double_precision_to_numeric_col NUMERIC,
  real_to_string_col STRING(MAX),
  binary_float_to_float64_col FLOAT64,
  binary_double_to_string_col STRING(MAX),
  integer_to_numeric_col NUMERIC,
  int_to_numeric_col NUMERIC,
  smallint_to_numeric_col NUMERIC,
  date_to_date_col DATE,
  timestamp_to_string_col STRING(MAX),
  timestamp_with_time_zone_to_string_col STRING(MAX),
  timestamp_with_local_time_zone_to_string_col STRING(MAX),
  raw_to_bytes_col BYTES(MAX),
  blob_to_string_col STRING(MAX),
  clob_to_bytes_col BYTES(MAX),
  nclob_to_bytes_col BYTES(MAX),
  boolean_to_int64_col INT64,
  json_to_string_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE varchar2_pk_table (
  varchar2_pk_col STRING(MAX) NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(varchar2_pk_col);

CREATE TABLE varchar_pk_table (
  varchar_pk_col STRING(MAX) NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(varchar_pk_col);

CREATE TABLE char_pk_table (
  char_pk_col STRING(MAX) NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(char_pk_col);

CREATE TABLE character_pk_table (
  character_pk_col STRING(MAX) NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(character_pk_col);

CREATE TABLE nvarchar2_pk_table (
  nvarchar2_pk_col STRING(MAX) NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(nvarchar2_pk_col);

CREATE TABLE nchar_pk_table (
  nchar_pk_col STRING(MAX) NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(nchar_pk_col);

CREATE TABLE nchar_varying_pk_table (
  nchar_varying_pk_col STRING(MAX) NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(nchar_varying_pk_col);

CREATE TABLE national_character_pk_table (
  national_character_pk_col STRING(MAX) NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(national_character_pk_col);

CREATE TABLE national_char_pk_table (
  national_char_pk_col STRING(MAX) NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(national_char_pk_col);

CREATE TABLE national_character_varying_pk_table (
  national_character_varying_pk_col STRING(MAX) NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(national_character_varying_pk_col);

CREATE TABLE national_char_varying_pk_table (
  national_char_varying_pk_col STRING(MAX) NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(national_char_varying_pk_col);

CREATE TABLE number_pk_table (
  number_pk_col NUMERIC NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(number_pk_col);

CREATE TABLE dec_pk_table (
  dec_pk_col NUMERIC NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(dec_pk_col);

CREATE TABLE integer_pk_table (
  integer_pk_col INT64 NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(integer_pk_col);

CREATE TABLE int_pk_table (
  int_pk_col INT64 NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(int_pk_col);

CREATE TABLE smallint_pk_table (
  smallint_pk_col INT64 NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(smallint_pk_col);

CREATE TABLE date_pk_table (
  date_pk_col TIMESTAMP NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(date_pk_col);

CREATE TABLE timestamp_pk_table (
  timestamp_pk_col TIMESTAMP NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(timestamp_pk_col);



CREATE TABLE raw_pk_table (
  raw_pk_col BYTES(MAX) NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(raw_pk_col);

CREATE TABLE boolean_pk_table (
  boolean_pk_col BOOL NOT NULL,
  val STRING(MAX)
) PRIMARY KEY(boolean_pk_col);


CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);
