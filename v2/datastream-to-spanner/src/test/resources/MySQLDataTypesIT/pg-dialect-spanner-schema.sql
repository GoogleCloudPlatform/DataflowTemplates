CREATE TABLE IF NOT EXISTS bigint_table (
  id INT8 NOT NULL,
  bigint_col INT8,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS bigint_to_string_table (
  id INT8 NOT NULL,
  bigint_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS bigint_unsigned_table (
  id INT8 NOT NULL,
  bigint_unsigned_col NUMERIC,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS binary_table (
  id INT8 NOT NULL,
  binary_col BYTEA,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS binary_to_string_table (
  id INT8 NOT NULL,
  binary_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS bit_table (
  id INT8 NOT NULL,
  bit_col BYTEA,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS bit_to_bool_table (
  id INT8 NOT NULL,
  bit_to_bool_col BOOL,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS bit_to_int64_table (
  id INT8 NOT NULL,
  bit_to_int64_col INT8,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS bit_to_string_table (
  id INT8 NOT NULL,
  bit_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS blob_table (
  id INT8 NOT NULL,
  blob_col BYTEA,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS blob_to_string_table (
  id INT8 NOT NULL,
  blob_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS bool_table (
  id INT8 NOT NULL,
  bool_col BOOL,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS bool_to_string_table (
  id INT8 NOT NULL,
  bool_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS boolean_table (
  id INT8 NOT NULL,
  boolean_col BOOL,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS boolean_to_bool_table (
  id INT8 NOT NULL,
  boolean_to_bool_col BOOL,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS boolean_to_string_table (
  id INT8 NOT NULL,
  boolean_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS char_table (
  id INT8 NOT NULL,
  char_col VARCHAR(255),
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS date_table (
  id INT8 NOT NULL,
  date_col DATE,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS date_to_string_table (
  id INT8 NOT NULL,
  date_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS datetime_table (
  id INT8 NOT NULL,
  datetime_col TIMESTAMPTZ,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS datetime_to_string_table (
  id INT8 NOT NULL,
  datetime_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS decimal_table (
  id INT8 NOT NULL,
  decimal_col NUMERIC,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS decimal_to_string_table (
  id INT8 NOT NULL,
  decimal_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dec_to_numeric_table (
  id INT8 NOT NULL,
  dec_to_numeric_col NUMERIC,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dec_to_string_table (
  id INT8 NOT NULL,
  dec_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS double_table (
  id INT8 NOT NULL,
  double_col FLOAT8,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS double_to_string_table (
  id INT8 NOT NULL,
  double_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS double_precision_to_float64_table (
  id INT8 NOT NULL,
  double_precision_to_float64_col FLOAT8,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS double_precision_to_string_table (
  id INT8 NOT NULL,
  double_precision_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS enum_table (
  id INT8 NOT NULL,
  enum_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS float_table (
  id INT8 NOT NULL,
  float_col FLOAT8,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS float_to_float32_table (
  id INT8 NOT NULL,
  float_to_float32_col FLOAT4,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS float_to_string_table (
  id INT8 NOT NULL,
  float_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS int_table (
  id INT8 NOT NULL,
  int_col INT8,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS int_to_string_table (
  id INT8 NOT NULL,
  int_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS integer_to_int64_table (
  id INT8 NOT NULL,
  integer_to_int64_col INT8,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS integer_to_string_table (
  id INT8 NOT NULL,
  integer_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS integer_unsigned_table (
  id INT8 NOT NULL,
  integer_unsigned_col INT8,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS smallint_unsigned_table (
  id INT8 NOT NULL,
  smallint_unsigned_col INT8,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS test_json_table (
  id INT8 NOT NULL,
  test_json_col JSONB,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS json_to_string_table (
  id INT8 NOT NULL,
  json_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS longblob_table (
  id INT8 NOT NULL,
  longblob_col BYTEA,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS longblob_to_string_table (
  id INT8 NOT NULL,
  longblob_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS longtext_table (
  id INT8 NOT NULL,
  longtext_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS mediumblob_table (
  id INT8 NOT NULL,
  mediumblob_col BYTEA,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS mediumblob_to_string_table (
  id INT8 NOT NULL,
  mediumblob_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS mediumint_table (
  id INT8 not null,
  mediumint_col INT8,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS mediumint_to_string_table (
  id INT8 NOT NULL,
  mediumint_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS mediumint_unsigned_table (
  id INT8 not null,
  mediumint_unsigned_col INT8,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS mediumtext_table (
  id INT8 NOT NULL,
  mediumtext_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS numeric_to_numeric_table (
  id INT8 NOT NULL,
  numeric_to_numeric_col NUMERIC,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS numeric_to_string_table (
  id INT8 NOT NULL,
  numeric_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS real_to_float64_table (
  id INT8 NOT NULL,
  real_to_float64_col FLOAT8,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS real_to_string_table (
  id INT8 NOT NULL,
  real_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS set_table (
  id INT8 NOT NULL,
  set_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS set_to_array_table (
  id INT8 NOT NULL,
  set_to_array_col VARCHAR[],
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS smallint_table (
  id INT8 NOT NULL,
  smallint_col INT8,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS smallint_to_string_table (
  id INT8 NOT NULL,
  smallint_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS text_table (
  id INT8 NOT NULL,
  text_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS time_table (
  id INT8 NOT NULL,
  time_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS timestamp_table (
  id INT8 NOT NULL,
  timestamp_col TIMESTAMPTZ,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS timestamp_to_string_table (
  id INT8 NOT NULL,
  timestamp_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS tinyblob_table (
  id INT8 NOT NULL,
  tinyblob_col BYTEA,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS tinyblob_to_string_table (
  id INT8 NOT NULL,
  tinyblob_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS tinyint_table (
  id INT8 NOT NULL,
  tinyint_col INT8,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS tinyint_to_string_table (
  id INT8 NOT NULL,
  tinyint_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS tinyint_unsigned_table (
  id INT8 NOT NULL,
  tinyint_unsigned_col INT8,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS tinytext_table (
  id INT8 NOT NULL,
  tinytext_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS varbinary_table (
  id INT8 NOT NULL,
  varbinary_col BYTEA,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS varbinary_to_string_table (
  id INT8 NOT NULL,
  varbinary_to_string_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS varchar_table (
  id INT8 NOT NULL,
  varchar_col VARCHAR(21000),
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS year_table (
  id INT8 NOT NULL,
  year_col VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS spatial_linestring (
  id INT8 NOT NULL,
  path VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS spatial_multilinestring (
  id INT8 NOT NULL,
  paths VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS spatial_multipoint (
  id INT8 NOT NULL,
  points VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS spatial_multipolygon (
  id INT8 NOT NULL,
  areas VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS spatial_point (
  id INT8 NOT NULL,
  location VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS spatial_polygon (
  id INT8 NOT NULL,
  area VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS spatial_geometry (
  id INT8 NOT NULL,
  geom VARCHAR,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS spatial_geometrycollection (
  id INT8 NOT NULL,
  geom_coll VARCHAR,
  PRIMARY KEY (id)
);
