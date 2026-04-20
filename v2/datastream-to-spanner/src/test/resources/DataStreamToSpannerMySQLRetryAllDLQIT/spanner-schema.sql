CREATE TABLE Customers (
    CustomerId INT64 NOT NULL,
    CustomerName STRING(255),
    CreditLimit NUMERIC,
    LegacyRegion STRING(50),
) PRIMARY KEY (CustomerId);

ALTER TABLE Customers ADD CONSTRAINT CHK_CreditLimit CHECK (CreditLimit > 1000);

CREATE TABLE Orders (
    CustomerId INT64 NOT NULL,
    OrderId INT64 NOT NULL,
    OrderValue NUMERIC,
    LegacyOrderSystem STRING(50) NOT NULL,
) PRIMARY KEY (CustomerId, LegacyOrderSystem, OrderId);

ALTER TABLE Orders ADD CONSTRAINT FK_CustomerOrder FOREIGN KEY (CustomerId) REFERENCES Customers(CustomerId);

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
    bigint_unsigned_col NUMERIC,
    float_col FLOAT64,
    double_col FLOAT64,
    decimal_col NUMERIC,
    datetime_col TIMESTAMP,
    time_col STRING(MAX),
    year_col STRING(MAX),
    char_col STRING(255),
    tinyblob_col BYTES(MAX),
    tinytext_col STRING(MAX),
    blob_col BYTES(MAX),
    mediumblob_col BYTES(MAX),
    mediumtext_col STRING(MAX),
    test_json_col JSON,
    longblob_col BYTES(MAX),
    longtext_col STRING(MAX),
    enum_col STRING(MAX),
    bool_col BOOL,
    binary_col BYTES(MAX),
    varbinary_col BYTES(MAX),
    bit_col BYTES(MAX),
    bit8_col INT64,
    bit1_col BOOL,
    boolean_col BOOL,
    int_col INT64,
    integer_unsigned_col INT64,
    timestamp_col TIMESTAMP,
    set_col STRING(MAX),
) PRIMARY KEY(id);
