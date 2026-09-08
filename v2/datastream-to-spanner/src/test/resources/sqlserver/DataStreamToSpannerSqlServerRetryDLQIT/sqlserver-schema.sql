CREATE TABLE Customers (
    CustomerId INT NOT NULL PRIMARY KEY,
    CustomerName VARCHAR(255),
    CreditLimit DECIMAL(10, 2) NOT NULL,
    LoyaltyTier VARCHAR(50)
);

CREATE TABLE Orders (
    CustomerId INT NOT NULL,
    OrderId INT NOT NULL,
    OrderValue DECIMAL(10, 2),
    OrderSource VARCHAR(50) NOT NULL,
    PRIMARY KEY (CustomerId, OrderId)
);

CREATE TABLE AllDataTypes (
    id INT PRIMARY KEY,
    varchar_col VARCHAR(1000) DEFAULT NULL,
    tinyint_col TINYINT DEFAULT NULL,
    text_col VARCHAR(MAX) DEFAULT NULL,
    date_col DATE DEFAULT NULL,
    smallint_col SMALLINT DEFAULT NULL,
    bigint_col BIGINT DEFAULT NULL,
    float_col FLOAT(53) DEFAULT NULL,
    decimal_col DECIMAL(38,9) DEFAULT NULL,
    datetime_col DATETIME DEFAULT NULL,
    time_col TIME DEFAULT NULL,
    char_col CHAR(255) DEFAULT NULL,
    binary_col BINARY(255) DEFAULT NULL,
    varbinary_col VARBINARY(1000) DEFAULT NULL,
    bit_col BIT DEFAULT NULL,
    int_col INT DEFAULT NULL
);
