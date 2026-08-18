CREATE TABLE AllDatatypes (
    id INT PRIMARY KEY,
    tinyint_col TINYINT,
    tinyint_bool_col TINYINT(1),
    smallint_col SMALLINT,
    mediumint_col MEDIUMINT,
    int_col INT,
    bigint_col BIGINT,
    decimal_col DECIMAL(65, 30),
    tinyint_unsigned_col TINYINT UNSIGNED,
    smallint_unsigned_col SMALLINT UNSIGNED,
    mediumint_unsigned_col MEDIUMINT UNSIGNED,
    int_unsigned_col INT UNSIGNED,
    bigint_unsigned_col BIGINT UNSIGNED,
    float_col FLOAT,
    double_col DOUBLE,
    bit_col BIT(64),
    bit_bool_col BIT(1),
    date_col DATE,
    time_col TIME(6),
    datetime_col DATETIME(6),
    timestamp_col TIMESTAMP(6) NULL DEFAULT NULL,
    year_col YEAR,
    char_col CHAR(255),
    varchar_col VARCHAR(2000),
    binary_col BINARY(255),
    varbinary_col VARBINARY(2000),
    tinyblob_col TINYBLOB,
    blob_col BLOB,
    mediumblob_col MEDIUMBLOB,
    longblob_col LONGBLOB,
    tinytext_col TINYTEXT,
    text_col TEXT,
    mediumtext_col MEDIUMTEXT,
    longtext_col LONGTEXT,
    enum_col ENUM('v1', 'v2', 'v3'),
    set_col SET('v1', 'v2', 'v3'),
    json_col JSON
);

-- Row 1 (Standard Values)
INSERT INTO AllDatatypes (
    id, tinyint_col, tinyint_bool_col, smallint_col, mediumint_col, int_col, bigint_col,
    tinyint_unsigned_col, smallint_unsigned_col, mediumint_unsigned_col, int_unsigned_col,
    float_col, double_col, datetime_col, timestamp_col, time_col, year_col, date_col,
    varchar_col, tinytext_col, text_col, mediumtext_col, longtext_col, char_col,
    binary_col, varbinary_col, tinyblob_col, blob_col, mediumblob_col, longblob_col,
    bit_col, bit_bool_col, enum_col, set_col, json_col
) VALUES (
    1, 42, 1, 12345, 5000000, 1000000000, 4000000000000000000,
    42, 12345, 5000000, 1000000000,
    123.0, 123.0, '2024-01-01 10:00:00', '2024-01-01 10:00:00', '10:00:00', 2024, '2024-01-01',
    'varchar', 'tinytext', 'text', 'mediumtext', 'longtext', 'standard',
    'standard_binary', 'standard_varbinary', 'standard_tinyblob', 'standard_blob', 'standard_mediumblob', 'standard_longblob',
    b'1', b'1', 'v1', 'v1,v2', '{}'
);

-- Row 2 (All NULL values)
INSERT INTO AllDatatypes (id) VALUES (2);

-- Row 3 (Minimum Values)
INSERT INTO AllDatatypes (
    id, tinyint_col, tinyint_bool_col, smallint_col, mediumint_col, int_col, bigint_col,
    tinyint_unsigned_col, smallint_unsigned_col, mediumint_unsigned_col, int_unsigned_col,
    float_col, double_col, bit_col, bit_bool_col, date_col, time_col, datetime_col, timestamp_col, year_col,
    char_col, varchar_col, binary_col, varbinary_col, tinyblob_col, blob_col, mediumblob_col, longblob_col,
    tinytext_col, text_col, mediumtext_col, longtext_col, enum_col, set_col, json_col
) VALUES (
    3, -128, 0, -32768, -8388608, -2147483648, -9223372036854775808,
    0, 0, 0, 0,
    -3.402823E+38, -1.7976931348623157E+308, x'0000000000000000', b'0', '1970-01-01', '-838:59:59.000000', '1970-01-01 00:00:00', '1970-01-01 00:00:01', 0,
    '', '', '', '', '', '', '', '',
    '', '', '', '', 'v1', '', '{}'
);

-- Row 4 (Maximum Values)
INSERT INTO AllDatatypes (
    id, tinyint_col, tinyint_bool_col, smallint_col, mediumint_col, int_col, bigint_col,
    tinyint_unsigned_col, smallint_unsigned_col, mediumint_unsigned_col, int_unsigned_col,
    float_col, double_col, bit_col, bit_bool_col, date_col, time_col, datetime_col, timestamp_col, year_col,
    char_col, varchar_col, binary_col, varbinary_col, tinyblob_col, blob_col, mediumblob_col, longblob_col,
    tinytext_col, text_col, mediumtext_col, longtext_col, enum_col, set_col
) VALUES (
    4, 127, 1, 32767, 8388607, 2147483647, 9223372036854775807,
    255, 65535, 16777215, 4294967295,
    3.402823E+38, 1.7976931348623157E+308, x'FFFFFFFFFFFFFFFF', b'1', '9999-12-31', '838:59:59.000000', '9999-12-31 23:59:59.999999', '2038-01-19 03:14:07.999999', 2155,
    REPEAT('Z', 255), REPEAT('Z', 2000), REPEAT(x'FF', 255), REPEAT(x'FF', 2000), REPEAT(x'FF', 255), REPEAT(x'FF', 65535), REPEAT(x'FF', 50000), REPEAT(x'FF', 50000),
    REPEAT('Z', 255), REPEAT('Z', 65535), REPEAT('Z', 50000), REPEAT('Z', 50000), 'v3', 'v1,v2,v3'
);
