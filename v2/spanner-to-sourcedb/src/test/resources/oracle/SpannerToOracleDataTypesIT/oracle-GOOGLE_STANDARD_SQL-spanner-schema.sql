CREATE TABLE varchar2_table (
  id INT64 NOT NULL,
  varchar2_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE varchar2_to_str4000_table (
  id INT64 NOT NULL,
  varchar2_to_str4000_col STRING(4000),
) PRIMARY KEY (id);

CREATE TABLE varchar2_to_bytes_table (
  id INT64 NOT NULL,
  varchar2_to_bytes_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE varchar2_pk_table (
  varchar2_pk_col STRING(MAX) NOT NULL,
) PRIMARY KEY (varchar2_pk_col);

CREATE TABLE varchar_table (
  id INT64 NOT NULL,
  varchar_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE varchar_to_str4000_table (
  id INT64 NOT NULL,
  varchar_to_str4000_col STRING(4000),
) PRIMARY KEY (id);

CREATE TABLE varchar_to_bytes_table (
  id INT64 NOT NULL,
  varchar_to_bytes_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE varchar_pk_table (
  varchar_pk_col STRING(MAX) NOT NULL,
) PRIMARY KEY (varchar_pk_col);

CREATE TABLE char_table (
  id INT64 NOT NULL,
  char_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE char_to_str2000_table (
  id INT64 NOT NULL,
  char_to_str2000_col STRING(2000),
) PRIMARY KEY (id);

CREATE TABLE char_to_bytes_table (
  id INT64 NOT NULL,
  char_to_bytes_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE char_pk_table (
  char_pk_col STRING(MAX) NOT NULL,
) PRIMARY KEY (char_pk_col);

CREATE TABLE character_table (
  id INT64 NOT NULL,
  character_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE character_to_str2000_table (
  id INT64 NOT NULL,
  character_to_str2000_col STRING(2000),
) PRIMARY KEY (id);

CREATE TABLE character_to_bytes_table (
  id INT64 NOT NULL,
  character_to_bytes_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE character_pk_table (
  character_pk_col STRING(MAX) NOT NULL,
) PRIMARY KEY (character_pk_col);

CREATE TABLE nvarchar2_table (
  id INT64 NOT NULL,
  nvarchar2_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE nvarchar2_to_str2000_table (
  id INT64 NOT NULL,
  nvarchar2_to_str2000_col STRING(2000),
) PRIMARY KEY (id);

CREATE TABLE nvarchar2_to_bytes_table (
  id INT64 NOT NULL,
  nvarchar2_to_bytes_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE nvarchar2_pk_table (
  nvarchar2_pk_col STRING(MAX) NOT NULL,
) PRIMARY KEY (nvarchar2_pk_col);

CREATE TABLE nchar_table (
  id INT64 NOT NULL,
  nchar_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE nchar_to_str1000_table (
  id INT64 NOT NULL,
  nchar_to_str1000_col STRING(1000),
) PRIMARY KEY (id);

CREATE TABLE nchar_to_bytes_table (
  id INT64 NOT NULL,
  nchar_to_bytes_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE nchar_pk_table (
  nchar_pk_col STRING(MAX) NOT NULL,
) PRIMARY KEY (nchar_pk_col);

CREATE TABLE nchar_varying_table (
  id INT64 NOT NULL,
  nchar_varying_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE nchar_varying_to_str2000_table (
  id INT64 NOT NULL,
  nchar_varying_to_str2000_col STRING(2000),
) PRIMARY KEY (id);

CREATE TABLE nchar_varying_to_bytes_table (
  id INT64 NOT NULL,
  nchar_varying_to_bytes_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE nchar_varying_pk_table (
  nchar_varying_pk_col STRING(MAX) NOT NULL,
) PRIMARY KEY (nchar_varying_pk_col);

CREATE TABLE nat_character_table (
  id INT64 NOT NULL,
  nat_character_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE nat_character_to_str1000_table (
  id INT64 NOT NULL,
  nat_character_to_str1000_col STRING(1000),
) PRIMARY KEY (id);

CREATE TABLE nat_character_to_bytes_table (
  id INT64 NOT NULL,
  nat_character_to_bytes_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE nat_character_pk_table (
  nat_character_pk_col STRING(MAX) NOT NULL,
) PRIMARY KEY (nat_character_pk_col);

CREATE TABLE nat_char_table (
  id INT64 NOT NULL,
  nat_char_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE nat_char_to_str1000_table (
  id INT64 NOT NULL,
  nat_char_to_str1000_col STRING(1000),
) PRIMARY KEY (id);

CREATE TABLE nat_char_to_bytes_table (
  id INT64 NOT NULL,
  nat_char_to_bytes_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE nat_char_pk_table (
  nat_char_pk_col STRING(MAX) NOT NULL,
) PRIMARY KEY (nat_char_pk_col);

CREATE TABLE nat_characvar_table (
  id INT64 NOT NULL,
  nat_characvar_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE nat_characvar_to_str2000_table (
  id INT64 NOT NULL,
  nat_characvar_to_str2000_col STRING(2000),
) PRIMARY KEY (id);

CREATE TABLE nat_characvar_to_bytes_table (
  id INT64 NOT NULL,
  nat_characvar_to_bytes_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE nat_characvar_pk_table (
  nat_characvar_pk_col STRING(MAX) NOT NULL,
) PRIMARY KEY (nat_characvar_pk_col);

CREATE TABLE nat_charvar_table (
  id INT64 NOT NULL,
  nat_charvar_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE nat_charvar_to_str2000_table (
  id INT64 NOT NULL,
  nat_charvar_to_str2000_col STRING(2000),
) PRIMARY KEY (id);

CREATE TABLE nat_charvar_to_bytes_table (
  id INT64 NOT NULL,
  nat_charvar_to_bytes_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE nat_charvar_pk_table (
  nat_charvar_pk_col STRING(MAX) NOT NULL,
) PRIMARY KEY (nat_charvar_pk_col);

CREATE TABLE number_table (
  id INT64 NOT NULL,
  number_col NUMERIC,
) PRIMARY KEY (id);

CREATE TABLE number_to_float64_table (
  id INT64 NOT NULL,
  number_to_float64_col FLOAT64,
) PRIMARY KEY (id);

CREATE TABLE number_to_str_table (
  id INT64 NOT NULL,
  number_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE number_to_int64_table (
  id INT64 NOT NULL,
  number_to_int64_col INT64,
) PRIMARY KEY (id);

CREATE TABLE numeric_table (
  id INT64 NOT NULL,
  numeric_col NUMERIC,
) PRIMARY KEY (id);

CREATE TABLE numeric_to_float64_table (
  id INT64 NOT NULL,
  numeric_to_float64_col FLOAT64,
) PRIMARY KEY (id);

CREATE TABLE numeric_to_str_table (
  id INT64 NOT NULL,
  numeric_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE numeric_to_int64_table (
  id INT64 NOT NULL,
  numeric_to_int64_col INT64,
) PRIMARY KEY (id);

CREATE TABLE decimal_table (
  id INT64 NOT NULL,
  decimal_col NUMERIC,
) PRIMARY KEY (id);

CREATE TABLE decimal_to_float64_table (
  id INT64 NOT NULL,
  decimal_to_float64_col FLOAT64,
) PRIMARY KEY (id);

CREATE TABLE decimal_to_str_table (
  id INT64 NOT NULL,
  decimal_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE decimal_to_int64_table (
  id INT64 NOT NULL,
  decimal_to_int64_col INT64,
) PRIMARY KEY (id);

CREATE TABLE dec_table (
  id INT64 NOT NULL,
  dec_col NUMERIC,
) PRIMARY KEY (id);

CREATE TABLE dec_to_float64_table (
  id INT64 NOT NULL,
  dec_to_float64_col FLOAT64,
) PRIMARY KEY (id);

CREATE TABLE dec_to_str_table (
  id INT64 NOT NULL,
  dec_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE dec_to_int64_table (
  id INT64 NOT NULL,
  dec_to_int64_col INT64,
) PRIMARY KEY (id);

CREATE TABLE float_table (
  id INT64 NOT NULL,
  float_col NUMERIC,
) PRIMARY KEY (id);

CREATE TABLE float_to_float64_table (
  id INT64 NOT NULL,
  float_to_float64_col FLOAT64,
) PRIMARY KEY (id);

CREATE TABLE float_to_str_table (
  id INT64 NOT NULL,
  float_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE float_to_int64_table (
  id INT64 NOT NULL,
  float_to_int64_col INT64,
) PRIMARY KEY (id);

CREATE TABLE double_prec_table (
  id INT64 NOT NULL,
  double_prec_col FLOAT64,
) PRIMARY KEY (id);

CREATE TABLE double_prec_to_numeric_table (
  id INT64 NOT NULL,
  double_prec_to_numeric_col NUMERIC,
) PRIMARY KEY (id);

CREATE TABLE double_prec_to_str_table (
  id INT64 NOT NULL,
  double_prec_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE double_prec_to_int64_table (
  id INT64 NOT NULL,
  double_prec_to_int64_col INT64,
) PRIMARY KEY (id);

CREATE TABLE real_table (
  id INT64 NOT NULL,
  real_col FLOAT64,
) PRIMARY KEY (id);

CREATE TABLE real_to_numeric_table (
  id INT64 NOT NULL,
  real_to_numeric_col NUMERIC,
) PRIMARY KEY (id);

CREATE TABLE real_to_str_table (
  id INT64 NOT NULL,
  real_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE real_to_int64_table (
  id INT64 NOT NULL,
  real_to_int64_col INT64,
) PRIMARY KEY (id);

CREATE TABLE binary_float_table (
  id INT64 NOT NULL,
  binary_float_col FLOAT32,
) PRIMARY KEY (id);

CREATE TABLE binary_float_to_float64_table (
  id INT64 NOT NULL,
  binary_float_to_float64_col FLOAT64,
) PRIMARY KEY (id);

CREATE TABLE binary_float_to_str_table (
  id INT64 NOT NULL,
  binary_float_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE binary_float_to_numeric_table (
  id INT64 NOT NULL,
  binary_float_to_numeric_col NUMERIC,
) PRIMARY KEY (id);

CREATE TABLE binary_float_to_int64_table (
  id INT64 NOT NULL,
  binary_float_to_int64_col INT64,
) PRIMARY KEY (id);

CREATE TABLE binary_double_table (
  id INT64 NOT NULL,
  binary_double_col FLOAT64,
) PRIMARY KEY (id);

CREATE TABLE binary_double_to_str_table (
  id INT64 NOT NULL,
  binary_double_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE binary_double_to_numeric_table (
  id INT64 NOT NULL,
  binary_double_to_numeric_col NUMERIC,
) PRIMARY KEY (id);

CREATE TABLE binary_double_to_int64_table (
  id INT64 NOT NULL,
  binary_double_to_int64_col INT64,
) PRIMARY KEY (id);

CREATE TABLE integer_table (
  id INT64 NOT NULL,
  integer_col INT64,
) PRIMARY KEY (id);

CREATE TABLE integer_to_numeric_table (
  id INT64 NOT NULL,
  integer_to_numeric_col NUMERIC,
) PRIMARY KEY (id);

CREATE TABLE integer_to_str_table (
  id INT64 NOT NULL,
  integer_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE integer_to_float64_table (
  id INT64 NOT NULL,
  integer_to_float64_col FLOAT64,
) PRIMARY KEY (id);

CREATE TABLE integer_pk_table (
  integer_pk_col INT64 NOT NULL,
) PRIMARY KEY (integer_pk_col);

CREATE TABLE int_table (
  id INT64 NOT NULL,
  int_col INT64,
) PRIMARY KEY (id);

CREATE TABLE int_to_numeric_table (
  id INT64 NOT NULL,
  int_to_numeric_col NUMERIC,
) PRIMARY KEY (id);

CREATE TABLE int_to_str_table (
  id INT64 NOT NULL,
  int_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE int_to_float64_table (
  id INT64 NOT NULL,
  int_to_float64_col FLOAT64,
) PRIMARY KEY (id);

CREATE TABLE int_pk_table (
  int_pk_col INT64 NOT NULL,
) PRIMARY KEY (int_pk_col);

CREATE TABLE smallint_table (
  id INT64 NOT NULL,
  smallint_col INT64,
) PRIMARY KEY (id);

CREATE TABLE smallint_to_numeric_table (
  id INT64 NOT NULL,
  smallint_to_numeric_col NUMERIC,
) PRIMARY KEY (id);

CREATE TABLE smallint_to_str_table (
  id INT64 NOT NULL,
  smallint_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE smallint_to_float64_table (
  id INT64 NOT NULL,
  smallint_to_float64_col FLOAT64,
) PRIMARY KEY (id);

CREATE TABLE smallint_pk_table (
  smallint_pk_col INT64 NOT NULL,
) PRIMARY KEY (smallint_pk_col);

CREATE TABLE date_table (
  id INT64 NOT NULL,
  date_col TIMESTAMP,
) PRIMARY KEY (id);

CREATE TABLE date_to_date_table (
  id INT64 NOT NULL,
  date_to_date_col DATE,
) PRIMARY KEY (id);

CREATE TABLE date_to_str_table (
  id INT64 NOT NULL,
  date_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE date_to_int64_table (
  id INT64 NOT NULL,
  date_to_int64_col INT64,
) PRIMARY KEY (id);

CREATE TABLE date_pk_table (
  date_pk_col TIMESTAMP NOT NULL,
) PRIMARY KEY (date_pk_col);

CREATE TABLE timestamp_table (
  id INT64 NOT NULL,
  timestamp_col TIMESTAMP,
) PRIMARY KEY (id);

CREATE TABLE timestamp_to_str_table (
  id INT64 NOT NULL,
  timestamp_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE timestamp_to_int64_table (
  id INT64 NOT NULL,
  timestamp_to_int64_col INT64,
) PRIMARY KEY (id);

CREATE TABLE timestamp_pk_table (
  timestamp_pk_col TIMESTAMP NOT NULL,
) PRIMARY KEY (timestamp_pk_col);

CREATE TABLE timestamp_tz_table (
  id INT64 NOT NULL,
  timestamp_tz_col TIMESTAMP,
) PRIMARY KEY (id);

CREATE TABLE timestamp_tz_to_str_table (
  id INT64 NOT NULL,
  timestamp_tz_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE timestamp_tz_to_int64_table (
  id INT64 NOT NULL,
  timestamp_tz_to_int64_col INT64,
) PRIMARY KEY (id);

CREATE TABLE timestamp_ltz_table (
  id INT64 NOT NULL,
  timestamp_ltz_col TIMESTAMP,
) PRIMARY KEY (id);

CREATE TABLE timestamp_ltz_to_str_table (
  id INT64 NOT NULL,
  timestamp_ltz_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE timestamp_ltz_to_int64_table (
  id INT64 NOT NULL,
  timestamp_ltz_to_int64_col INT64,
) PRIMARY KEY (id);

CREATE TABLE timestamp_ltz_pk_table (
  timestamp_ltz_pk_col TIMESTAMP NOT NULL,
) PRIMARY KEY (timestamp_ltz_pk_col);

CREATE TABLE raw_table (
  id INT64 NOT NULL,
  raw_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE raw_to_bytes2000_table (
  id INT64 NOT NULL,
  raw_to_bytes2000_col BYTES(2000),
) PRIMARY KEY (id);

CREATE TABLE raw_to_str_table (
  id INT64 NOT NULL,
  raw_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE raw_pk_table (
  raw_pk_col BYTES(MAX) NOT NULL,
) PRIMARY KEY (raw_pk_col);

CREATE TABLE blob_table (
  id INT64 NOT NULL,
  blob_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE blob_to_str_table (
  id INT64 NOT NULL,
  blob_to_str_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE clob_table (
  id INT64 NOT NULL,
  clob_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE clob_to_bytes_table (
  id INT64 NOT NULL,
  clob_to_bytes_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE nclob_table (
  id INT64 NOT NULL,
  nclob_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE nclob_to_bytes_table (
  id INT64 NOT NULL,
  nclob_to_bytes_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE interval_year_to_month_table (
  id INT64 NOT NULL,
  interval_year_to_month_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE interval_day_to_second_table (
  id INT64 NOT NULL,
  interval_day_to_second_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE long_raw_table (
  id INT64 NOT NULL,
  long_raw_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE bfile_table (
  id INT64 NOT NULL,
  bfile_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE long_table (
  id INT64 NOT NULL,
  long_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE rowid_table (
  id INT64 NOT NULL,
  rowid_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE urowid_table (
  id INT64 NOT NULL,
  urowid_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE xmltype_table (
  id INT64 NOT NULL,
  xmltype_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE anydata_table (
  id INT64 NOT NULL,
  anydata_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE anytype_table (
  id INT64 NOT NULL,
  anytype_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE anydataset_table (
  id INT64 NOT NULL,
  anydataset_col BYTES(MAX),
) PRIMARY KEY (id);

CREATE TABLE uritype_table (
  id INT64 NOT NULL,
  uritype_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE dburitype_table (
  id INT64 NOT NULL,
  dburitype_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE xdburitype_table (
  id INT64 NOT NULL,
  xdburitype_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE httpuritype_table (
  id INT64 NOT NULL,
  httpuritype_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE mlslabel_table (
  id INT64 NOT NULL,
  mlslabel_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE ref_table (
  id INT64 NOT NULL,
  ref_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE varray_table (
  id INT64 NOT NULL,
  varray_col ARRAY<STRING(MAX)>,
) PRIMARY KEY (id);

CREATE TABLE nested_table_table (
  id INT64 NOT NULL,
  nested_table_col JSON,
) PRIMARY KEY (id);

CREATE TABLE object_type_table (
  id INT64 NOT NULL,
  object_type_col JSON,
) PRIMARY KEY (id);

CREATE CHANGE STREAM allstream FOR ALL OPTIONS (value_capture_type = 'NEW_ROW', retention_period = '7d', allow_txn_exclusion = true);
