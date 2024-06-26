CREATE TABLE AllDatatypeTransformation (
    varchar_column VARCHAR(20) NOT NULL,
    tinyint_column TINYINT,
    text_column TEXT,
    date_column DATE,
    int_column INT,
    bigint_column BIGINT,
    float_column FLOAT,
    double_column DOUBLE,
    decimal_column DECIMAL(18,9),
    datetime_column DATETIME,
    timestamp_column TIMESTAMP,
    time_column TIME,
    year_column YEAR,
    blob_column BLOB,
    enum_column ENUM('1', '2', '3'),
    bool_column BOOLEAN,
    binary_col BINARY(255),
    bit_column BIT(10),
    varbinary_column VARBINARY(100),
    char_col CHAR(255),
    longblob_col LONGBLOB,
    longtext_col LONGTEXT,
    mediumblob_col MEDIUMBLOB,
    mediumint_col MEDIUMINT,
    mediumtext_col MEDIUMTEXT,
    set_col SET('v1', 'v2', 'v3'),
    smallint_col SMALLINT,
    tinyblob_col TINYBLOB,
    tinytext_col TINYTEXT,
    json_col JSON,
    PRIMARY KEY (int_column)
);



INSERT INTO AllDatatypeTransformation (
    varchar_column, tinyint_column, text_column, date_column, int_column,
    bigint_column, float_column, double_column, decimal_column, datetime_column,
    timestamp_column, time_column, year_column, blob_column, enum_column,
    bool_column, varbinary_column, bit_column, binary_col, char_col, longblob_col,
    longtext_col, mediumblob_col, mediumint_col, mediumtext_col, set_col, smallint_col,
    tinyblob_col, tinytext_col, json_col
)
VALUES (
    'id1', 12, 'This is a text value', '2024-06-21', 100,
    134567890, 3.14159, 2.71828, 12345.6789, '2024-06-21 17:10:01',
    '2022-12-31 23:59:58', '17:10:00', '2024', x'7835383030', '2',
    false, x'7835383030000000000000000000000000000000', 42,x'7835383030000000000000000000000000000000',
    'a', x'7835383030', 'This is longtext', x'7835383030', 2000, 'This is mediumtext',
    'v1,v2', 10, x'7835383030', 'This is tinytext', '{"k1": "v1"}'
);

