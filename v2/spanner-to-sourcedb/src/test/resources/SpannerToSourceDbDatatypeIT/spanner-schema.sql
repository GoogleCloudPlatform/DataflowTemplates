CREATE TABLE IF NOT EXISTS AllDatatypeColumns (
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
    other_bool_column BOOL,
    binary_column BYTES(MAX),
    varbinary_column BYTES(20),
    bit_column BYTES(MAX),
    null_string_column STRING(128),
    null_int_column INT64,
    null_date_column DATE,
    null_float_64_column FLOAT64,
    null_float_32_column FLOAT32,
    null_numeric_column NUMERIC,
    null_timestamp_column TIMESTAMP,
    null_blob_column BYTES(20),
    null_bool_column BOOL,
) PRIMARY KEY(varchar_column);



CREATE TABLE IF NOT EXISTS AllDatatypePkColumns1 (
    uuid_column STRING(128) NOT NULL,
    varchar_column STRING(20),
    tinyint_column INT64,
    text_column STRING(MAX),
    date_column DATE,
    smallint_column INT64,
    mediumint_column INT64,
    int_column INT64,
    bigint_column INT64,
    float_column FLOAT64,
    double_column FLOAT64,
    decimal_column NUMERIC,
    datetime_column TIMESTAMP,
    timestamp_column TIMESTAMP,
    time_column STRING(MAX),
    year_column STRING(MAX)
) PRIMARY KEY(uuid_column);

CREATE TABLE IF NOT EXISTS AllDatatypePkColumns2 (
    uuid_column STRING(128) NOT NULL,
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
    other_bool_column BOOL,
    binary_column BYTES(MAX),
    varbinary_column BYTES(20),
    bit_column BYTES(MAX)
) PRIMARY KEY(uuid_column);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d',
  allow_txn_exclusion = true
);