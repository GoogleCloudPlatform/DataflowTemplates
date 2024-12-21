CREATE TABLE IF NOT EXISTS AllDatatypeColumns (
  varchar_column STRING(20) NOT NULL,
  tinyint_column STRING(MAX),
  text_column BYTES(MAX),
  date_column STRING(MAX),
  smallint_column STRING(MAX),
  mediumint_column STRING(MAX),
  int_column STRING(MAX),
  bigint_column STRING(MAX),
  float_column STRING(MAX),
  double_column STRING(MAX),
  decimal_column STRING(MAX),
  datetime_column STRING(MAX),
  timestamp_column STRING(MAX),
  time_column STRING(MAX),
  year_column STRING(MAX),
  char_column BYTES(10),
  tinyblob_column STRING(MAX),
  tinytext_column BYTES(MAX),
  blob_column STRING(MAX),
  mediumblob_column STRING(MAX),
  mediumtext_column BYTES(MAX),
  longblob_column STRING(MAX),
  longtext_column BYTES(MAX),
  enum_column STRING(MAX),
  bool_column INT64,
  other_bool_column STRING(MAX),
  binary_column STRING(MAX),
  varbinary_column STRING(MAX),
  bit_column STRING(MAX),
) PRIMARY KEY(varchar_column);

CREATE TABLE IF NOT EXISTS AllDatatypeColumns2 (
  varchar_column STRING(20) NOT NULL,
  tinyint_column INT64,
  text_column STRING(MAX),
  date_column DATE,
  smallint_column INT64,
  mediumint_column INT64,
  int_column INT64,
  bigint_column INT64,
  float_column FLOAT32,
  double_column FLOAT64,
  decimal_column NUMERIC,
  datetime_column TIMESTAMP,
  timestamp_column TIMESTAMP,
  time_column STRING(MAX),
  year_column STRING(MAX),
  char_column STRING(10),
  tinyblob_column BYTES(MAX),
  tinytext_column STRING(MAX),
  blob_column BYTES(MAX),
  mediumblob_column BYTES(MAX),
  mediumtext_column STRING(MAX),
  longblob_column BYTES(MAX),
  longtext_column STRING(MAX),
  enum_column STRING(MAX),
  bool_column BOOL,
  binary_column BYTES(MAX),
  varbinary_column BYTES(MAX),
  bit_column BYTES(MAX),
) PRIMARY KEY(varchar_column);

CREATE TABLE IF NOT EXISTS DatatypeColumnsWithSizes (
   varchar_column STRING(30) NOT NULL,
   float_column FLOAT32,
   decimal_column NUMERIC,
   char_column STRING(50),
   bool_column BOOL,
   binary_column BYTES(30),
   varbinary_column BYTES(30),
   bit_column BYTES(10),
) PRIMARY KEY(varchar_column);

CREATE TABLE IF NOT EXISTS DatatypeColumnsReducedSizes (
    varchar_column STRING(10) NOT NULL,
    float_column FLOAT32,
    decimal_column NUMERIC,
    char_column STRING(20),
    bool_column BOOL,
    binary_column BYTES(MAX),
    varbinary_column BYTES(MAX),
    bit_column BYTES(MAX),
) PRIMARY KEY(varchar_column);

CREATE TABLE Users (
   user_id INT64 NOT NULL,
   first_name STRING(50),
   last_name STRING(50),
   age INT64,
   full_name STRING(100) AS (ARRAY_TO_STRING([first_name, last_name], " ")) STORED,
) PRIMARY KEY (user_id);

CREATE TABLE Authors (
   id INT64 NOT NULL,
   name STRING(200),
) PRIMARY KEY (id);

CREATE TABLE AllDatatypeTransformation (
	varchar_column STRING(20) NOT NULL,
	tinyint_column INT64,
	text_column STRING(MAX),
	date_column DATE,
	int_column INT64,
	bigint_column INT64,
	float_column FLOAT32,
	double_column FLOAT64,
	decimal_column NUMERIC,
	datetime_column TIMESTAMP,
	timestamp_column TIMESTAMP,
	time_column STRING(MAX),
	year_column STRING(MAX),
	blob_column BYTES(MAX),
	enum_column STRING(MAX),
	bool_column BOOL,
	binary_column BYTES(MAX),
	bit_column BYTES(MAX),
) PRIMARY KEY (varchar_column);

CREATE SEQUENCE sequence_id OPTIONS (sequence_kind='bit_reversed_positive', skip_range_min = 0, skip_range_max = 3);

CREATE TABLE Singers (
    singer_id INT64 NOT NULL  DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE sequence_id)) ,
    first_name STRING(1024),
    last_name STRING(1024),
) PRIMARY KEY (singer_id);
