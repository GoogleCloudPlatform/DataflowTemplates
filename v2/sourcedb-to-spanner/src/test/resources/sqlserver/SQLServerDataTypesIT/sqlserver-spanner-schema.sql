-- Spanner Google Standard SQL schema for SQLServer DataTypesIT

-- ============================================================================
-- Scenario A: Default Type Migration
-- ============================================================================

CREATE TABLE IF NOT EXISTS tinyint_table (
  id INT64 NOT NULL,
  tinyint_col INT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS smallint_table (
  id INT64 NOT NULL,
  smallint_col INT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS int_table (
  id INT64 NOT NULL,
  int_col INT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS bigint_table (
  id INT64 NOT NULL,
  bigint_col INT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS bit_table (
  id INT64 NOT NULL,
  bit_col BOOL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS decimal_table (
  id INT64 NOT NULL,
  decimal_col NUMERIC,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS numeric_table (
  id INT64 NOT NULL,
  numeric_col NUMERIC,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS money_table (
  id INT64 NOT NULL,
  money_col NUMERIC,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS smallmoney_table (
  id INT64 NOT NULL,
  smallmoney_col NUMERIC,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS float_table (
  id INT64 NOT NULL,
  float_col FLOAT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS real_table (
  id INT64 NOT NULL,
  real_col FLOAT32,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS date_table (
  id INT64 NOT NULL,
  date_col DATE,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS time_table (
  id INT64 NOT NULL,
  time_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS datetime2_table (
  id INT64 NOT NULL,
  datetime2_col TIMESTAMP,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS datetimeoffset_table (
  id INT64 NOT NULL,
  datetimeoffset_col TIMESTAMP,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS datetime_table (
  id INT64 NOT NULL,
  datetime_col TIMESTAMP,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS smalldatetime_table (
  id INT64 NOT NULL,
  smalldatetime_col TIMESTAMP,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS char_table (
  id INT64 NOT NULL,
  char_col STRING(10),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS varchar_table (
  id INT64 NOT NULL,
  varchar_col STRING(255),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS varchar_max_table (
  id INT64 NOT NULL,
  varchar_max_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS text_table (
  id INT64 NOT NULL,
  text_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS nchar_table (
  id INT64 NOT NULL,
  nchar_col STRING(10),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS nvarchar_table (
  id INT64 NOT NULL,
  nvarchar_col STRING(255),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS nvarchar_max_table (
  id INT64 NOT NULL,
  nvarchar_max_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS ntext_table (
  id INT64 NOT NULL,
  ntext_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS binary_table (
  id INT64 NOT NULL,
  binary_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS varbinary_table (
  id INT64 NOT NULL,
  varbinary_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS varbinary_max_table (
  id INT64 NOT NULL,
  varbinary_max_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS image_table (
  id INT64 NOT NULL,
  image_col BYTES(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS uniqueidentifier_table (
  id INT64 NOT NULL,
  uniqueidentifier_col STRING(36),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS xml_table (
  id INT64 NOT NULL,
  xml_col STRING(MAX),
) PRIMARY KEY(id);


-- ============================================================================
-- Scenario B: Alternative Type Migration
-- ============================================================================

CREATE TABLE IF NOT EXISTS tinyint_to_string_table (
  id INT64 NOT NULL,
  tinyint_to_string_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS smallint_to_string_table (
  id INT64 NOT NULL,
  smallint_to_string_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS int_to_string_table (
  id INT64 NOT NULL,
  int_to_string_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS bigint_to_string_table (
  id INT64 NOT NULL,
  bigint_to_string_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS bit_to_int64_table (
  id INT64 NOT NULL,
  bit_to_int64_col INT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS decimal_to_float64_table (
  id INT64 NOT NULL,
  decimal_to_float64_col FLOAT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS decimal_to_string_table (
  id INT64 NOT NULL,
  decimal_to_string_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS numeric_to_float64_table (
  id INT64 NOT NULL,
  numeric_to_float64_col FLOAT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS numeric_to_string_table (
  id INT64 NOT NULL,
  numeric_to_string_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS money_to_float64_table (
  id INT64 NOT NULL,
  money_to_float64_col FLOAT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS money_to_string_table (
  id INT64 NOT NULL,
  money_to_string_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS smallmoney_to_float64_table (
  id INT64 NOT NULL,
  smallmoney_to_float64_col FLOAT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS smallmoney_to_string_table (
  id INT64 NOT NULL,
  smallmoney_to_string_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS float_to_string_table (
  id INT64 NOT NULL,
  float_to_string_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS real_to_float64_table (
  id INT64 NOT NULL,
  real_to_float64_col FLOAT64,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS real_to_string_table (
  id INT64 NOT NULL,
  real_to_string_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS date_to_string_table (
  id INT64 NOT NULL,
  date_to_string_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS datetime2_to_string_table (
  id INT64 NOT NULL,
  datetime2_to_string_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS datetime_to_string_table (
  id INT64 NOT NULL,
  datetime_to_string_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS smalldatetime_to_string_table (
  id INT64 NOT NULL,
  smalldatetime_to_string_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS binary_to_string_table (
  id INT64 NOT NULL,
  binary_to_string_col STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS varbinary_to_string_table (
  id INT64 NOT NULL,
  varbinary_to_string_col STRING(MAX),
) PRIMARY KEY(id);


-- ============================================================================
-- Scenario C: Primary Key Mapping
-- ============================================================================

CREATE TABLE IF NOT EXISTS tinyint_pk_table (
  id INT64 NOT NULL,
  val STRING(50),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS smallint_pk_table (
  id INT64 NOT NULL,
  val STRING(50),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS int_pk_table (
  id INT64 NOT NULL,
  val STRING(50),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS bigint_pk_table (
  id INT64 NOT NULL,
  val STRING(50),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS bit_pk_table (
  id BOOL NOT NULL,
  val STRING(50),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS date_pk_table (
  id DATE NOT NULL,
  val STRING(50),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS char_pk_table (
  id STRING(10) NOT NULL,
  val STRING(50),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS varchar_pk_table (
  id STRING(50) NOT NULL,
  val STRING(50),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS nchar_pk_table (
  id STRING(10) NOT NULL,
  val STRING(50),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS nvarchar_pk_table (
  id STRING(50) NOT NULL,
  val STRING(50),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS binary_pk_table (
  id BYTES(4) NOT NULL,
  val STRING(50),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS varbinary_pk_table (
  id BYTES(50) NOT NULL,
  val STRING(50),
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS uniqueidentifier_pk_table (
  id STRING(36) NOT NULL,
  val STRING(50),
) PRIMARY KEY(id);


-- ============================================================================
-- Scenario D: Unsupported / Complex Types
-- ============================================================================

CREATE TABLE IF NOT EXISTS geography_table (
  id INT64 NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS geometry_table (
  id INT64 NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS hierarchyid_table (
  id INT64 NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS sql_variant_table (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
