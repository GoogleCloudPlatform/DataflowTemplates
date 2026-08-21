
CREATE TABLE varchar2_table (
  id INT64 NOT NULL,
  varchar2_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE varchar2_to_string_table (
  id INT64 NOT NULL,
  varchar2_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE varchar2_to_bytes_table (
  id INT64 NOT NULL,
  varchar2_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE varchar_table (
  id INT64 NOT NULL,
  varchar_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE varchar_to_string_table (
  id INT64 NOT NULL,
  varchar_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE varchar_to_bytes_table (
  id INT64 NOT NULL,
  varchar_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE char_table (
  id INT64 NOT NULL,
  char_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE char_to_string_table (
  id INT64 NOT NULL,
  char_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE char_to_bytes_table (
  id INT64 NOT NULL,
  char_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE character_table (
  id INT64 NOT NULL,
  character_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE character_to_string_table (
  id INT64 NOT NULL,
  character_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE character_to_bytes_table (
  id INT64 NOT NULL,
  character_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE nvarchar2_table (
  id INT64 NOT NULL,
  nvarchar2_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE nvarchar2_to_string_table (
  id INT64 NOT NULL,
  nvarchar2_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE nvarchar2_to_bytes_table (
  id INT64 NOT NULL,
  nvarchar2_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE nchar_table (
  id INT64 NOT NULL,
  nchar_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE nchar_to_string_table (
  id INT64 NOT NULL,
  nchar_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE nchar_to_bytes_table (
  id INT64 NOT NULL,
  nchar_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE nchar_varying_table (
  id INT64 NOT NULL,
  nchar_varying_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE nchar_varying_to_string_table (
  id INT64 NOT NULL,
  nchar_varying_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE nchar_varying_to_bytes_table (
  id INT64 NOT NULL,
  nchar_varying_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE national_character_table (
  id INT64 NOT NULL,
  national_character_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE national_character_to_string_table (
  id INT64 NOT NULL,
  national_character_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE national_character_to_bytes_table (
  id INT64 NOT NULL,
  national_character_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE national_char_table (
  id INT64 NOT NULL,
  national_char_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE national_char_to_string_table (
  id INT64 NOT NULL,
  national_char_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE national_char_to_bytes_table (
  id INT64 NOT NULL,
  national_char_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE national_character_varying_table (
  id INT64 NOT NULL,
  national_character_varying_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE national_character_varying_to_string_table (
  id INT64 NOT NULL,
  national_character_varying_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE national_character_varying_to_bytes_table (
  id INT64 NOT NULL,
  national_character_varying_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE national_char_varying_table (
  id INT64 NOT NULL,
  national_char_varying_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE national_char_varying_to_string_table (
  id INT64 NOT NULL,
  national_char_varying_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE national_char_varying_to_bytes_table (
  id INT64 NOT NULL,
  national_char_varying_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE number_table (
  id INT64 NOT NULL,
  number_col FLOAT64
) PRIMARY KEY(id);

CREATE TABLE number_to_numeric_table (
  id INT64 NOT NULL,
  number_col NUMERIC
) PRIMARY KEY(id);

CREATE TABLE number_to_string_table (
  id INT64 NOT NULL,
  number_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE number_to_int64_table (
  id INT64 NOT NULL,
  number_col INT64
) PRIMARY KEY(id);

CREATE TABLE numeric_table (
  id INT64 NOT NULL,
  numeric_col NUMERIC
) PRIMARY KEY(id);

CREATE TABLE numeric_to_float64_table (
  id INT64 NOT NULL,
  numeric_col FLOAT64
) PRIMARY KEY(id);

CREATE TABLE numeric_to_string_table (
  id INT64 NOT NULL,
  numeric_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE numeric_to_int64_table (
  id INT64 NOT NULL,
  numeric_col INT64
) PRIMARY KEY(id);

CREATE TABLE decimal_table (
  id INT64 NOT NULL,
  decimal_col NUMERIC
) PRIMARY KEY(id);

CREATE TABLE decimal_to_float64_table (
  id INT64 NOT NULL,
  decimal_col FLOAT64
) PRIMARY KEY(id);

CREATE TABLE decimal_to_string_table (
  id INT64 NOT NULL,
  decimal_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE decimal_to_int64_table (
  id INT64 NOT NULL,
  decimal_col INT64
) PRIMARY KEY(id);

CREATE TABLE dec_table (
  id INT64 NOT NULL,
  dec_col NUMERIC
) PRIMARY KEY(id);

CREATE TABLE dec_to_float64_table (
  id INT64 NOT NULL,
  dec_col FLOAT64
) PRIMARY KEY(id);

CREATE TABLE dec_to_string_table (
  id INT64 NOT NULL,
  dec_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE dec_to_int64_table (
  id INT64 NOT NULL,
  dec_col INT64
) PRIMARY KEY(id);

CREATE TABLE float_table (
  id INT64 NOT NULL,
  float_col FLOAT64
) PRIMARY KEY(id);

CREATE TABLE float_to_numeric_table (
  id INT64 NOT NULL,
  float_col NUMERIC
) PRIMARY KEY(id);

CREATE TABLE float_to_string_table (
  id INT64 NOT NULL,
  float_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE float_to_int64_table (
  id INT64 NOT NULL,
  float_col INT64
) PRIMARY KEY(id);

CREATE TABLE double_precision_table (
  id INT64 NOT NULL,
  double_precision_col FLOAT64
) PRIMARY KEY(id);

CREATE TABLE double_precision_to_numeric_table (
  id INT64 NOT NULL,
  double_precision_col NUMERIC
) PRIMARY KEY(id);

CREATE TABLE double_precision_to_string_table (
  id INT64 NOT NULL,
  double_precision_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE double_precision_to_int64_table (
  id INT64 NOT NULL,
  double_precision_col INT64
) PRIMARY KEY(id);

CREATE TABLE real_table (
  id INT64 NOT NULL,
  real_col FLOAT64
) PRIMARY KEY(id);

CREATE TABLE real_to_numeric_table (
  id INT64 NOT NULL,
  real_col NUMERIC
) PRIMARY KEY(id);

CREATE TABLE real_to_string_table (
  id INT64 NOT NULL,
  real_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE real_to_int64_table (
  id INT64 NOT NULL,
  real_col INT64
) PRIMARY KEY(id);

CREATE TABLE binary_float_table (
  id INT64 NOT NULL,
  binary_float_col FLOAT64
) PRIMARY KEY(id);

CREATE TABLE binary_float_to_float64_table (
  id INT64 NOT NULL,
  binary_float_col FLOAT64
) PRIMARY KEY(id);

CREATE TABLE binary_float_to_string_table (
  id INT64 NOT NULL,
  binary_float_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE binary_float_to_numeric_table (
  id INT64 NOT NULL,
  binary_float_col NUMERIC
) PRIMARY KEY(id);

CREATE TABLE binary_double_table (
  id INT64 NOT NULL,
  binary_double_col FLOAT64
) PRIMARY KEY(id);

CREATE TABLE binary_double_to_string_table (
  id INT64 NOT NULL,
  binary_double_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE binary_double_to_numeric_table (
  id INT64 NOT NULL,
  binary_double_col NUMERIC
) PRIMARY KEY(id);

CREATE TABLE integer_table (
  id INT64 NOT NULL,
  integer_col INT64
) PRIMARY KEY(id);

CREATE TABLE integer_to_numeric_table (
  id INT64 NOT NULL,
  integer_col NUMERIC
) PRIMARY KEY(id);

CREATE TABLE integer_to_string_table (
  id INT64 NOT NULL,
  integer_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE integer_to_float64_table (
  id INT64 NOT NULL,
  integer_col FLOAT64
) PRIMARY KEY(id);

CREATE TABLE integer_pk_table (
  integer_pk_col INT64 NOT NULL
) PRIMARY KEY(integer_pk_col);

CREATE TABLE int_table (
  id INT64 NOT NULL,
  int_col INT64
) PRIMARY KEY(id);

CREATE TABLE int_to_numeric_table (
  id INT64 NOT NULL,
  int_col NUMERIC
) PRIMARY KEY(id);

CREATE TABLE int_to_string_table (
  id INT64 NOT NULL,
  int_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE int_to_float64_table (
  id INT64 NOT NULL,
  int_col FLOAT64
) PRIMARY KEY(id);

CREATE TABLE int_pk_table (
  int_pk_col INT64 NOT NULL
) PRIMARY KEY(int_pk_col);

CREATE TABLE smallint_table (
  id INT64 NOT NULL,
  smallint_col INT64
) PRIMARY KEY(id);

CREATE TABLE smallint_to_numeric_table (
  id INT64 NOT NULL,
  smallint_col NUMERIC
) PRIMARY KEY(id);

CREATE TABLE smallint_to_string_table (
  id INT64 NOT NULL,
  smallint_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE smallint_to_float64_table (
  id INT64 NOT NULL,
  smallint_col FLOAT64
) PRIMARY KEY(id);

CREATE TABLE smallint_pk_table (
  smallint_pk_col INT64 NOT NULL
) PRIMARY KEY(smallint_pk_col);

CREATE TABLE date_table (
  id INT64 NOT NULL,
  date_col TIMESTAMP
) PRIMARY KEY(id);

CREATE TABLE date_to_date_table (
  id INT64 NOT NULL,
  date_col DATE
) PRIMARY KEY(id);

CREATE TABLE date_to_string_table (
  id INT64 NOT NULL,
  date_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE date_to_int64_table (
  id INT64 NOT NULL,
  date_col INT64
) PRIMARY KEY(id);

CREATE TABLE date_pk_table (
  date_pk_col TIMESTAMP NOT NULL
) PRIMARY KEY(date_pk_col);

CREATE TABLE timestamp_table (
  id INT64 NOT NULL,
  timestamp_col TIMESTAMP
) PRIMARY KEY(id);

CREATE TABLE timestamp_to_string_table (
  id INT64 NOT NULL,
  timestamp_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE timestamp_to_int64_table (
  id INT64 NOT NULL,
  timestamp_col INT64
) PRIMARY KEY(id);

CREATE TABLE timestamp_pk_table (
  timestamp_pk_col TIMESTAMP NOT NULL
) PRIMARY KEY(timestamp_pk_col);

CREATE TABLE interval_year_to_month_table (
  id INT64 NOT NULL,
  interval_year_to_month_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE interval_year_to_month_to_bigint_months_table (
  id INT64 NOT NULL,
  interval_year_to_month_col INT64
) PRIMARY KEY(id);

CREATE TABLE interval_year_to_month_to_float64_table (
  id INT64 NOT NULL,
  interval_year_to_month_col FLOAT64
) PRIMARY KEY(id);

CREATE TABLE interval_day_to_second_table (
  id INT64 NOT NULL,
  interval_day_to_second_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE interval_day_to_second_to_bigint_millis_table (
  id INT64 NOT NULL,
  interval_day_to_second_col INT64
) PRIMARY KEY(id);

CREATE TABLE interval_day_to_second_to_float64_table (
  id INT64 NOT NULL,
  interval_day_to_second_col FLOAT64
) PRIMARY KEY(id);

CREATE TABLE raw_table (
  id INT64 NOT NULL,
  raw_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE raw_to_bytes_table (
  id INT64 NOT NULL,
  raw_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE raw_to_varchar_base64_table (
  id INT64 NOT NULL,
  raw_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE long_raw_table (
  id INT64 NOT NULL,
  long_raw_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE long_raw_to_varchar_base64_table (
  id INT64 NOT NULL,
  long_raw_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE blob_table (
  id INT64 NOT NULL,
  blob_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE blob_to_varchar_base64_table (
  id INT64 NOT NULL,
  blob_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE clob_table (
  id INT64 NOT NULL,
  clob_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE clob_to_bytes_table (
  id INT64 NOT NULL,
  clob_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE nclob_table (
  id INT64 NOT NULL,
  nclob_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE nclob_to_bytes_table (
  id INT64 NOT NULL,
  nclob_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE bfile_table (
  id INT64 NOT NULL,
  bfile_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE bfile_to_bytes_table (
  id INT64 NOT NULL,
  bfile_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE bfile_to_varchar_url_table (
  id INT64 NOT NULL,
  bfile_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE long_table (
  id INT64 NOT NULL,
  long_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE long_to_bytes_table (
  id INT64 NOT NULL,
  long_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE rowid_table (
  id INT64 NOT NULL,
  rowid_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE rowid_to_bytes_table (
  id INT64 NOT NULL,
  rowid_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE rowid_to_int64_table (
  id INT64 NOT NULL,
  rowid_col INT64
) PRIMARY KEY(id);

CREATE TABLE urowid_table (
  id INT64 NOT NULL,
  urowid_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE urowid_to_bytes_table (
  id INT64 NOT NULL,
  urowid_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE urowid_to_int64_table (
  id INT64 NOT NULL,
  urowid_col INT64
) PRIMARY KEY(id);

CREATE TABLE boolean_table (
  id INT64 NOT NULL,
  boolean_col BOOL
) PRIMARY KEY(id);

CREATE TABLE boolean_to_bigint_0_1_table (
  id INT64 NOT NULL,
  boolean_col INT64
) PRIMARY KEY(id);

CREATE TABLE boolean_to_varchar_true_false_table (
  id INT64 NOT NULL,
  boolean_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE json_table (
  id INT64 NOT NULL,
  json_col JSON
) PRIMARY KEY(id);

CREATE TABLE json_to_string_table (
  id INT64 NOT NULL,
  json_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE json_to_bytes_table (
  id INT64 NOT NULL,
  json_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE xmltype_table (
  id INT64 NOT NULL,
  xmltype_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE xmltype_to_bytes_table (
  id INT64 NOT NULL,
  xmltype_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE sdo_geometry_table (
  id INT64 NOT NULL,
  sdo_geometry_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE sdo_geometry_to_bytes_table (
  id INT64 NOT NULL,
  sdo_geometry_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE sdo_geometry_to_json_table (
  id INT64 NOT NULL,
  sdo_geometry_col JSON
) PRIMARY KEY(id);

CREATE TABLE sdo_topo_geometry_table (
  id INT64 NOT NULL,
  sdo_topo_geometry_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE sdo_topo_geometry_to_bytes_table (
  id INT64 NOT NULL,
  sdo_topo_geometry_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE sdo_topo_geometry_to_json_table (
  id INT64 NOT NULL,
  sdo_topo_geometry_col JSON
) PRIMARY KEY(id);

CREATE TABLE sdo_georaster_table (
  id INT64 NOT NULL,
  sdo_georaster_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE sdo_georaster_to_varchar_uri_table (
  id INT64 NOT NULL,
  sdo_georaster_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE vector_table (
  id INT64 NOT NULL,
  vector_col ARRAY<FLOAT64>
) PRIMARY KEY(id);

CREATE TABLE vector_to_array_double_precision_table (
  id INT64 NOT NULL,
  vector_col ARRAY<FLOAT64>
) PRIMARY KEY(id);

CREATE TABLE vector_to_bytes_table (
  id INT64 NOT NULL,
  vector_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE anydata_table (
  id INT64 NOT NULL,
  anydata_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE anydata_to_string_table (
  id INT64 NOT NULL,
  anydata_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE anytype_table (
  id INT64 NOT NULL,
  anytype_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE anytype_to_string_table (
  id INT64 NOT NULL,
  anytype_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE anydataset_table (
  id INT64 NOT NULL,
  anydataset_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE anydataset_to_string_table (
  id INT64 NOT NULL,
  anydataset_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE uritype_table (
  id INT64 NOT NULL,
  uritype_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE uritype_to_bytes_table (
  id INT64 NOT NULL,
  uritype_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE dburitype_table (
  id INT64 NOT NULL,
  dburitype_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE dburitype_to_bytes_table (
  id INT64 NOT NULL,
  dburitype_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE xdburitype_table (
  id INT64 NOT NULL,
  xdburitype_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE xdburitype_to_bytes_table (
  id INT64 NOT NULL,
  xdburitype_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE httpuritype_table (
  id INT64 NOT NULL,
  httpuritype_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE httpuritype_to_bytes_table (
  id INT64 NOT NULL,
  httpuritype_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE expression_table (
  id INT64 NOT NULL,
  expression_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE expression_to_bytes_table (
  id INT64 NOT NULL,
  expression_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE ordaudio_table (
  id INT64 NOT NULL,
  ordaudio_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE ordaudio_to_varchar_uri_table (
  id INT64 NOT NULL,
  ordaudio_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE ordimage_table (
  id INT64 NOT NULL,
  ordimage_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE ordimage_to_varchar_uri_table (
  id INT64 NOT NULL,
  ordimage_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE ordvideo_table (
  id INT64 NOT NULL,
  ordvideo_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE ordvideo_to_varchar_uri_table (
  id INT64 NOT NULL,
  ordvideo_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE orddoc_table (
  id INT64 NOT NULL,
  orddoc_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE orddoc_to_varchar_uri_table (
  id INT64 NOT NULL,
  orddoc_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE si_stillimage_table (
  id INT64 NOT NULL,
  si_stillimage_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE si_stillimage_to_varchar_jsonb_table (
  id INT64 NOT NULL,
  si_stillimage_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE si_color_table (
  id INT64 NOT NULL,
  si_color_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE si_color_to_string_table (
  id INT64 NOT NULL,
  si_color_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE si_averagecolor_table (
  id INT64 NOT NULL,
  si_averagecolor_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE si_averagecolor_to_string_table (
  id INT64 NOT NULL,
  si_averagecolor_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE si_colorhistogram_table (
  id INT64 NOT NULL,
  si_colorhistogram_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE si_colorhistogram_to_string_table (
  id INT64 NOT NULL,
  si_colorhistogram_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE si_positionalcolor_table (
  id INT64 NOT NULL,
  si_positionalcolor_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE si_positionalcolor_to_string_table (
  id INT64 NOT NULL,
  si_positionalcolor_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE si_texture_table (
  id INT64 NOT NULL,
  si_texture_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE si_texture_to_string_table (
  id INT64 NOT NULL,
  si_texture_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE si_featurelist_table (
  id INT64 NOT NULL,
  si_featurelist_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE si_featurelist_to_string_table (
  id INT64 NOT NULL,
  si_featurelist_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE mlslabel_table (
  id INT64 NOT NULL,
  mlslabel_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE mlslabel_to_bytes_table (
  id INT64 NOT NULL,
  mlslabel_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE ref_table (
  id INT64 NOT NULL,
  ref_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE ref_to_bytes_table (
  id INT64 NOT NULL,
  ref_col BYTES(MAX)
) PRIMARY KEY(id);

CREATE TABLE ref_cursor_table (
  id INT64 NOT NULL,
  ref_cursor_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE ref_cursor_to_unsupported_table (
  id INT64 NOT NULL,
  ref_cursor_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE varray_table (
  id INT64 NOT NULL,
  varray_col ARRAY<STRING(MAX)>
) PRIMARY KEY(id);

CREATE TABLE varray_to_json_table (
  id INT64 NOT NULL,
  varray_col JSON
) PRIMARY KEY(id);

CREATE TABLE nested_table_table (
  id INT64 NOT NULL,
  nested_table_col JSON
) PRIMARY KEY(id);

CREATE TABLE nested_table_to_string_table (
  id INT64 NOT NULL,
  nested_table_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE associative_array_table (
  id INT64 NOT NULL,
  associative_array_col JSON
) PRIMARY KEY(id);

CREATE TABLE associative_array_to_string_table (
  id INT64 NOT NULL,
  associative_array_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE object_type_table (
  id INT64 NOT NULL,
  object_type_col JSON
) PRIMARY KEY(id);

CREATE TABLE object_type_to_string_table (
  id INT64 NOT NULL,
  object_type_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE timestamp_with_time_zone_table (
  id INT64 NOT NULL,
  timestamp_with_time_zone_col TIMESTAMP
) PRIMARY KEY(id);

CREATE TABLE timestamp_with_time_zone_to_string_table (
  id INT64 NOT NULL,
  timestamp_with_time_zone_to_varchar_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE timestamp_with_time_zone_to_int64_table (
  id INT64 NOT NULL,
  timestamp_with_time_zone_to_bigint_col INT64
) PRIMARY KEY(id);

CREATE TABLE timestamp_with_local_time_zone_table (
  id INT64 NOT NULL,
  timestamp_with_local_time_zone_col TIMESTAMP
) PRIMARY KEY(id);

CREATE TABLE timestamp_with_local_time_zone_to_string_table (
  id INT64 NOT NULL,
  timestamp_with_local_time_zone_to_varchar_col STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE timestamp_with_local_time_zone_to_int64_table (
  id INT64 NOT NULL,
  timestamp_with_local_time_zone_to_bigint_col INT64
) PRIMARY KEY(id);
