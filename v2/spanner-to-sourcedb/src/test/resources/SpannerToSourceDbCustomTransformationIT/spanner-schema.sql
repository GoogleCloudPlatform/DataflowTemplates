CREATE TABLE IF NOT EXISTS Users1 (
    id INT64 NOT NULL,
    name STRING(25),
) PRIMARY KEY(id);

CREATE TABLE AllDatatypeTransformation (
	varchar_column STRING(20) NOT NULL,
	tinyint_column INT64,
	text_column STRING(MAX),
	date_column DATE,
	int_column INT64,
	bigint_column INT64,
	float_column FLOAT64,
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

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);