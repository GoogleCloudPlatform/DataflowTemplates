CREATE TABLE IF NOT EXISTS bigint_table (
  id INT64 NOT NULL,
  bigint_col int64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS bigint_to_string_table (
  id INT64 NOT NULL,
  bigint_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS bigint_unsigned_table (
  id INT64 NOT NULL,
  bigint_unsigned_col NUMERIC,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS binary_table (
  id INT64 NOT NULL,
  binary_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS binary_to_string_table (
  id INT64 NOT NULL,
  binary_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS bit_table (
  id INT64 NOT NULL,
  bit_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS bit_to_bool_table (
  id INT64 NOT NULL,
  bit_to_bool_col BOOL,
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS bit_to_int64_table (
  id INT64 NOT NULL,
  bit_to_int64_col INT64,
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS bit_to_string_table (
  id INT64 NOT NULL,
  bit_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS blob_table (
  id INT64 NOT NULL,
  blob_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS blob_to_string_table (
  id INT64 NOT NULL,
  blob_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS bool_table (
  id INT64 NOT NULL,
  bool_col BOOL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS bool_to_string_table (
  id INT64 NOT NULL,
  bool_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS boolean_table (
  id INT64 NOT NULL,
  boolean_col BOOL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS boolean_to_bool_table (
  id INT64 NOT NULL,
  boolean_to_bool_col BOOL,
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS boolean_to_string_table (
  id INT64 NOT NULL,
  boolean_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS char_table (
  id INT64 NOT NULL,
  char_col STRING(255),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS date_table (
  id INT64 NOT NULL,
  date_col DATE,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS date_to_string_table (
  id INT64 NOT NULL,
  date_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS datetime_table (
  id INT64 NOT NULL,
  datetime_col TIMESTAMP,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS datetime_to_string_table (
  id INT64 NOT NULL,
  datetime_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS decimal_table (
  id INT64 NOT NULL,
  decimal_col NUMERIC,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS decimal_to_string_table (
  id INT64 NOT NULL,
  decimal_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS dec_to_numeric_table (
  id INT64 NOT NULL,
  dec_to_numeric_col NUMERIC,
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS dec_to_string_table (
  id INT64 NOT NULL,
  dec_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS double_table (
  id INT64 NOT NULL,
  double_col FLOAT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS double_to_string_table (
  id INT64 NOT NULL,
  double_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS double_precision_to_float64_table (
  id INT64 NOT NULL,
  double_precision_to_float64_col FLOAT64,
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS double_precision_to_string_table (
  id INT64 NOT NULL,
  double_precision_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS enum_table (
  id INT64 NOT NULL,
  enum_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS float_table (
  id INT64 NOT NULL,
  float_col FLOAT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS float_to_float32_table (
  id INT64 NOT NULL,
  float_to_float32_col FLOAT32,
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS float_to_string_table (
  id INT64 NOT NULL,
  float_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS int_table (
  id INT64 NOT NULL,
  int_col INT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS int_to_string_table (
  id INT64 NOT NULL,
  int_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS integer_to_int64_table (
  id INT64 NOT NULL,
  integer_to_int64_col INT64,
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS integer_to_string_table (
  id INT64 NOT NULL,
  integer_to_string_col STRING(MAX),
) PRIMARY KEY (id);

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

CREATE TABLE IF NOT EXISTS json_to_string_table (
  id INT64 NOT NULL,
  json_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS longblob_table (
  id INT64 NOT NULL,
  longblob_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS longblob_to_string_table (
  id INT64 NOT NULL,
  longblob_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS longtext_table (
  id INT64 NOT NULL,
  longtext_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS mediumblob_table (
  id INT64 NOT NULL,
  mediumblob_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS mediumblob_to_string_table (
  id INT64 NOT NULL,
  mediumblob_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS mediumint_table (
  id int64 not null,
  mediumint_col int64,
) primary key(id);

CREATE TABLE IF NOT EXISTS mediumint_to_string_table (
  id INT64 NOT NULL,
  mediumint_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS mediumint_unsigned_table (
  id int64 not null,
  mediumint_unsigned_col int64,
) primary key(id);

CREATE TABLE IF NOT EXISTS mediumtext_table (
  id INT64 NOT NULL,
  mediumtext_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS numeric_to_numeric_table (
  id INT64 NOT NULL,
  numeric_to_numeric_col NUMERIC,
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS numeric_to_string_table (
  id INT64 NOT NULL,
  numeric_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS real_to_float64_table (
  id INT64 NOT NULL,
  real_to_float64_col FLOAT64,
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS real_to_string_table (
  id INT64 NOT NULL,
  real_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS set_table (
  id INT64 NOT NULL,
  set_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS set_to_array_table (
  id INT64 NOT NULL,
  set_to_array_col ARRAY<STRING(MAX)>,
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS smallint_table (
  id INT64 NOT NULL,
  smallint_col INT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS smallint_to_string_table (
  id INT64 NOT NULL,
  smallint_to_string_col STRING(MAX),
) PRIMARY KEY (id);

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

CREATE TABLE IF NOT EXISTS timestamp_to_string_table (
  id INT64 NOT NULL,
  timestamp_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS tinyblob_table (
  id INT64 NOT NULL,
  tinyblob_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS tinyblob_to_string_table (
  id INT64 NOT NULL,
  tinyblob_to_string_col STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS tinyint_table (
  id INT64 NOT NULL,
  tinyint_col INT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS tinyint_to_string_table (
  id INT64 NOT NULL,
  tinyint_to_string_col STRING(MAX),
) PRIMARY KEY (id);

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

CREATE TABLE IF NOT EXISTS varbinary_to_string_table (
  id INT64 NOT NULL,
  varbinary_to_string_col STRING(MAX),
) PRIMARY KEY (id);

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

CREATE TABLE IF NOT EXISTS spatial_geometry (
  id INT64 NOT NULL,
  geom STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS spatial_geometrycollection (
  id INT64 NOT NULL,
  geom_coll STRING(MAX),
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS `generated_pk_column_table` ( 
	`first_name_col` STRING(50),
	`last_name_col` STRING(50) DEFAULT(NULL),
	`generated_column_col` STRING(100) AS (concat(`first_name_col`,' ')) STORED,	
) PRIMARY KEY (`generated_column_col`);

CREATE TABLE IF NOT EXISTS `generated_non_pk_column_table` ( 
	`first_name_col` STRING(50),
	`last_name_col` STRING(50) DEFAULT(NULL),
	`generated_column_col` STRING(100) AS (concat(`first_name_col`,' ')) STORED,
  `id` INT64 not null,
) PRIMARY KEY (`id`);

CREATE TABLE IF NOT EXISTS `non_generated_to_generated_column_table` ( 
	`first_name_col` STRING(50),
	`last_name_col` STRING(50) DEFAULT(NULL),
	`generated_column_col` STRING(100) AS (concat(`first_name_col`,' ')) STORED,
	`generated_column_pk_col` STRING(100) AS (concat(`first_name_col`,' ')) STORED,
) PRIMARY KEY (`generated_column_pk_col`);

CREATE TABLE IF NOT EXISTS `generated_to_non_generated_column_table` ( 
	`first_name_col` STRING(50),
	`last_name_col` STRING(50) DEFAULT(NULL),
	`generated_column_col` STRING(100) DEFAULT(NULL),
	`generated_column_pk_col` STRING(100) DEFAULT(NULL),
) PRIMARY KEY (`generated_column_pk_col`);
