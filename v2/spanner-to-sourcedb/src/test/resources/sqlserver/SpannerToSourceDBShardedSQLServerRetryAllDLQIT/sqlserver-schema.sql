CREATE TABLE Customers (
    CustomerId INT NOT NULL PRIMARY KEY,
    CustomerName VARCHAR(255),
    CreditLimit DECIMAL(10, 2) NOT NULL,
    LegacyRegion VARCHAR(50),
    CONSTRAINT CHK_CreditLimit CHECK (CreditLimit > 1000)
);

CREATE TABLE Orders (
    CustomerId INT NOT NULL,
    OrderId INT NOT NULL,
    OrderValue DECIMAL(10, 2),
    LegacyOrderSystem VARCHAR(50) NOT NULL,
    PRIMARY KEY (CustomerId, LegacyOrderSystem, OrderId),
    CONSTRAINT FK_CustomerOrder FOREIGN KEY (CustomerId) REFERENCES Customers(CustomerId)
);

CREATE TABLE AllDataTypes (
    id INT PRIMARY KEY,
    varchar_col VARCHAR(1000) NULL,
    tinyint_col TINYINT NULL,
    tinyint_unsigned_col TINYINT NULL,
    text_col TEXT NULL,
    date_col DATE NULL,
    smallint_col SMALLINT NULL,
    smallint_unsigned_col SMALLINT NULL,
    mediumint_col INT NULL,
    mediumint_unsigned_col INT NULL,
    bigint_col BIGINT NULL,
    bigint_unsigned_col BIGINT NULL,
    float_col FLOAT NULL,
    double_col FLOAT NULL,
    decimal_col DECIMAL(38,10) NULL,
    datetime_col DATETIME NULL,
    time_col TIME NULL,
    year_col INT NULL,
    char_col CHAR(255) NULL,
    tinyblob_col VARBINARY(MAX) NULL,
    tinytext_col VARCHAR(MAX) NULL,
    blob_col VARBINARY(MAX) NULL,
    mediumblob_col VARBINARY(MAX) NULL,
    mediumtext_col VARCHAR(MAX) NULL,
    test_json_col VARCHAR(MAX) NULL,
    longblob_col VARBINARY(MAX) NULL,
    longtext_col VARCHAR(MAX) NULL,
    enum_col VARCHAR(50) NULL,
    bool_col BIT NULL,
    binary_col BINARY(255) NULL,
    varbinary_col VARBINARY(1000) NULL,
    bit_col BIGINT NULL,
    bit8_col TINYINT NULL,
    bit1_col BIT NULL,
    boolean_col BIT NULL,
    int_col INT NULL,
    integer_unsigned_col BIGINT NULL,
    timestamp_col DATETIME NULL,
    set_col VARCHAR(50) NULL
);
