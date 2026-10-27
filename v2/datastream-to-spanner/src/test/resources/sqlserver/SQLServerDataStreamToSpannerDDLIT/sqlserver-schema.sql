CREATE TABLE AllDatatypeTransformation (
    varchar_column VARCHAR(20) NOT NULL PRIMARY KEY,
    tinyint_column TINYINT,
    text_column VARCHAR(MAX),
    date_column DATE,
    int_column INT,
    bigint_column BIGINT,
    float_column REAL,
    double_column FLOAT,
    decimal_column DECIMAL(10, 2),
    datetime_column DATETIMEOFFSET,
    timestamp_column DATETIMEOFFSET,
    time_column VARCHAR(8),
    year_column SMALLINT,
    blob_column VARBINARY(MAX),
    enum_column VARCHAR(10),
    bool_column BIT,
    binary_column BINARY(20),
    bit_column TINYINT
);

CREATE TABLE DatatypeColumnsWithSizes (
    varchar_column VARCHAR(20) NOT NULL PRIMARY KEY,
    float_column REAL,
    decimal_column DECIMAL(10, 2),
    char_column CHAR(10),
    nvarchar_column NVARCHAR(20),
    bool_column BIT,
    binary_column BINARY(10),
    varbinary_column VARBINARY(20),
    bit_column TINYINT
);

CREATE TABLE Books (
    id INT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
    title VARCHAR(200)
);
