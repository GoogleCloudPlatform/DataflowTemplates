CREATE TABLE Customers (
    CustomerId INT64 NOT NULL,
    CustomerName STRING(255),
    CreditLimit NUMERIC,         -- No constraint
    LoyaltyTier STRING(50),      -- Renamed from LegacyRegion of MySQL
) PRIMARY KEY (CustomerId);

CREATE TABLE Orders (
    CustomerId INT64 NOT NULL,
    OrderId INT64 NOT NULL,
    OrderValue NUMERIC,
    OrderSource STRING(50) NOT NULL, -- Added column, NOT part of Spanner PK
) PRIMARY KEY (CustomerId, OrderId);

CREATE TABLE AllDataTypes (
    id INT64 NOT NULL,
    varchar_col STRING(1000),
    tinyint_col INT64,
    tinyint_unsigned_col INT64,
    text_col STRING(MAX),
    date_col DATE,
    smallint_col INT64,
    smallint_unsigned_col INT64,
    mediumint_col INT64,
    mediumint_unsigned_col INT64,
    bigint_col INT64,
    bigint_unsigned_col INT64,
    float_col FLOAT32,
    double_col FLOAT64,
    decimal_col NUMERIC,
    datetime_col TIMESTAMP,
    time_col STRING(MAX),
    year_col STRING(MAX),
    char_col STRING(255),
    tinyblob_col BYTES(255),
    tinytext_col STRING(MAX),
    blob_col BYTES(65535),
    mediumblob_col BYTES(10485760),
    mediumtext_col STRING(MAX),
    test_json_col JSON,
    longblob_col BYTES(10485760),
    longtext_col STRING(MAX),
    enum_col STRING(MAX),
    bool_col BOOL,
    binary_col BYTES(255),
    varbinary_col BYTES(1000),
    bit_col BYTES(MAX),
    bit8_col BYTES(MAX),
    bit1_col BOOL,
    boolean_col BOOL,
    int_col INT64,
    integer_unsigned_col INT64,
    timestamp_col TIMESTAMP,
    set_col STRING(MAX),
) PRIMARY KEY (id);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d',
  allow_txn_exclusion = true
);