## AllDatatypeColumns
INSERT INTO AllDatatypeColumns (
    varchar_column, tinyint_column, text_column, date_column, smallint_column,
    mediumint_column, int_column, bigint_column, float_column, double_column,
    decimal_column, datetime_column, timestamp_column, time_column, year_column,
    char_column, tinyblob_column, tinytext_column, blob_column, mediumblob_column,
    mediumtext_column, longblob_column, longtext_column, enum_column, bool_column,
    other_bool_column, binary_column, varbinary_column, bit_column
) VALUES (
             'value1', 10, UNHEX('746578745f646174615f310a'), '2024-02-08', 50,
             1000, 50000, 987654321, 45.67, 123.789, 456.12,
             '2024-02-08 08:15:30', '2024-02-08 08:15:30', SEC_TO_TIME(29730), 2022,
             UNHEX('63686172310a'), UNHEX('74696e79626c6f625f646174615f31'),
             UNHEX('74696e79746578745f646174615f310a'), UNHEX('626c6f625f646174615f31'),
             UNHEX('6d656469756d626c6f625f646174615f31'),
             UNHEX('6d656469756d746578745f646174615f31'),
             UNHEX('6c6f6e67626c6f625f646174615f31'),
             UNHEX('6c6f6e67746578745f646174615f31'), '2', FALSE, TRUE,
             UNHEX('62696e6172795f31'), UNHEX('76617262696e6172795f646174615f31'), b'1100110'
         );

INSERT INTO AllDatatypeColumns (
    varchar_column, tinyint_column, text_column, date_column, smallint_column,
    mediumint_column, int_column, bigint_column, float_column, double_column,
    decimal_column, datetime_column, timestamp_column, time_column, year_column,
    char_column, tinyblob_column, tinytext_column, blob_column, mediumblob_column,
    mediumtext_column, longblob_column, longtext_column, enum_column, bool_column,
    other_bool_column, binary_column, varbinary_column, bit_column
) VALUES (
             'value2', 5, UNHEX('746578745f646174615f320a'), '2024-02-09', 25,
             500, 25000, 987654, 12.34, 56.789, 123.45,
             '2024-02-09 15:30:45', '2024-02-09 15:30:45', SEC_TO_TIME(55845), 2023,
             UNHEX('63686172320a'), UNHEX('74696e79626c6f625f646174615f32'),
             UNHEX('74696e79746578745f646174615f320a'), UNHEX('626c6f625f646174615f32'),
             UNHEX('6d656469756d626c6f625f646174615f32'),
             UNHEX('6d656469756d746578745f646174615f32'),
             UNHEX('6c6f6e67626c6f625f646174615f32'),
             UNHEX('6c6f6e67746578745f646174615f32'), '3', TRUE, FALSE,
             UNHEX('62696e6172795f32'),UNHEX('76617262696e6172795f646174615f32'), b'11001'
         );

DELETE FROM AllDatatypeColumns where varchar_column = "value2";

UPDATE AllDatatypeColumns
SET
    tinyint_column = 15,
    text_column = UNHEX('746578745f646174615f310a'),
    date_column = '2024-02-08',
    smallint_column = 50,
    mediumint_column = 1000,
    int_column = 50000,
    bigint_column = 987654321,
    float_column = 45.67,
    double_column = 123.789,
    decimal_column = 456.12,
    datetime_column = '2024-02-08 08:15:30',
    timestamp_column = '2024-02-08 08:15:30',
    time_column = SEC_TO_TIME(29730),
    year_column = 2022,
    char_column = UNHEX('63686172310a'),
    tinyblob_column = UNHEX('74696e79626c6f625f646174615f31'),
    tinytext_column = UNHEX('74696e79746578745f646174615f310a'),
    blob_column = UNHEX('626c6f625f646174615f31'),
    mediumblob_column = UNHEX('6d656469756d626c6f625f646174615f31'),
    mediumtext_column = UNHEX('6d656469756d746578745f646174615f31'),
    longblob_column = UNHEX('6c6f6e67626c6f625f646174615f31'),
    longtext_column = UNHEX('6c6f6e67746578745f646174615f31'),
    enum_column = '2',
    bool_column = FALSE,
    other_bool_column = TRUE,
    binary_column = UNHEX('62696e6172795f31'),
    varbinary_column = UNHEX('76617262696e6172795f646174615f31'),
    bit_column = b'1100110'
WHERE varchar_column = 'value1';

## AllDatatypeColumns2
INSERT INTO AllDatatypeColumns2 (
    varchar_column, tinyint_column, text_column, date_column, smallint_column,
    mediumint_column, int_column, bigint_column, float_column, double_column,
    decimal_column, datetime_column, timestamp_column, time_column, year_column,
    char_column, tinyblob_column, tinytext_column, blob_column, mediumblob_column,
    mediumtext_column, longblob_column, longtext_column, enum_column, bool_column,
    binary_column, varbinary_column, bit_column
) VALUES (
             'value1', 10, 'text1', '2024-02-08', 50,
             1000, 50000, 987654321, 45.67, 123.789, 456.12,
             '2024-02-08 08:15:30', '2024-02-08 08:15:30', SEC_TO_TIME(29730), 2022,
             'char_1', UNHEX('74696e79626c6f625f646174615f31'), 'tinytext_data_1',
             UNHEX('626c6f625f646174615f31'), UNHEX('6d656469756d626c6f625f646174615f31'),
             'mediumtext_data_1', UNHEX('6c6f6e67626c6f625f646174615f31'), 'longtext_data_1',
             '2', FALSE,
             UNHEX('62696e6172795f646174615f3100000000000000'), UNHEX('76617262696e6172795f646174615f31'),
             b'1100110'
         );

INSERT INTO AllDatatypeColumns2 (
    varchar_column, tinyint_column, text_column, date_column, smallint_column,
    mediumint_column, int_column, bigint_column, float_column, double_column,
    decimal_column, datetime_column, timestamp_column, time_column, year_column,
    char_column, tinyblob_column, tinytext_column, blob_column, mediumblob_column,
    mediumtext_column, longblob_column, longtext_column, enum_column, bool_column,
    binary_column, varbinary_column, bit_column
) VALUES (
             'value2', 5, 'text2', '2024-02-09', 25,
             500, 25000, 987654, 12.34, 56.789, 123.45,
             '2024-02-09 15:30:45', '2024-02-09 15:30:45', SEC_TO_TIME(55845), 2023,
             'char_2', UNHEX('74696e79626c6f625f646174615f32'), 'tinytext_data_2',
             UNHEX('626c6f625f646174615f32'), UNHEX('6d656469756d626c6f625f646174615f32'),
             'mediumtext_data_2', UNHEX('6c6f6e67626c6f625f646174615f32'), 'longtext_data_2',
             '3', TRUE,
             UNHEX('62696e6172795f646174615f3200000000000000'), UNHEX('76617262696e6172795f646174615f32'),
             b'11001'
         );

UPDATE AllDatatypeColumns2
SET
    tinyint_column = 15,
    text_column = 'text1',
    date_column = '2024-02-08',
    smallint_column = 50,
    mediumint_column = 1000,
    int_column = 50000,
    bigint_column = 987654321,
    float_column = 45.67,
    double_column = 123.789,
    decimal_column = 456.12,
    datetime_column = '2024-02-08 08:15:30',
    timestamp_column = '2024-02-08 08:15:30',
    time_column = SEC_TO_TIME(29730),
    year_column = 2022,
    char_column = 'char_1',
    tinyblob_column = UNHEX('74696e79626c6f625f646174615f31'),
    tinytext_column = 'tinytext_data_1',
    blob_column = UNHEX('626c6f625f646174615f31'),
    mediumblob_column = UNHEX('6d656469756d626c6f625f646174615f31'),
    mediumtext_column = 'mediumtext_data_1',
    longblob_column = UNHEX('6c6f6e67626c6f625f646174615f31'),
    longtext_column = 'longtext_data_1',
    enum_column = '2',
    bool_column = FALSE,
    binary_column = UNHEX('62696e6172795f646174615f3100000000000000'),
    varbinary_column = UNHEX('76617262696e6172795f646174615f31'),
    bit_column = b'1100110'
WHERE varchar_column = 'value1';

DELETE FROM AllDatatypeColumns2 WHERE varchar_column = 'value2';

## DatatypeColumnsReducedSizes
INSERT INTO DatatypeColumnsReducedSizes (
    varchar_column, float_column, decimal_column, char_column, bool_column,
    binary_column, varbinary_column, bit_column
) VALUES (
             'value12345678901', 45.67, 456.12, 'char_1', FALSE,
             UNHEX('62696e6172795f646174615f3100000000000000'), UNHEX('76617262696e6172795f646174615f31'),
             b'1100110'
         );
INSERT INTO DatatypeColumnsReducedSizes (
    varchar_column, float_column, decimal_column, char_column, bool_column,
    binary_column, varbinary_column, bit_column
) VALUES (
             'value2', 12.34, 123.45, 'char_2', TRUE,
             UNHEX('62696e6172795f646174615f3200000000000000'), UNHEX('76617262696e6172795f646174615f32'),
             b'11001'
         );

## DatatypeColumnsWithSizes
INSERT INTO DatatypeColumnsWithSizes (
    varchar_column, float_column, decimal_column, char_column, bool_column,
    binary_column, varbinary_column, bit_column
) VALUES (
             'value1', 45.67, 456.12, 'char_1', FALSE,
             UNHEX('62696e6172795f646174615f3100000000000000'), UNHEX('76617262696e6172795f646174615f31'),
             b'1100110'
         );
INSERT INTO DatatypeColumnsWithSizes (
    varchar_column, float_column, decimal_column, char_column, bool_column,
    binary_column, varbinary_column, bit_column
) VALUES (
             'value2', 12.34, 123.45, 'char_2', TRUE,
             UNHEX('62696e6172795f646174615f3200000000000000'), UNHEX('76617262696e6172795f646174615f32'),
             b'11001'
         );

## Users
Insert into Users values(1, 'Lorem', 'Epsum', 20); Insert into Users values(2, 'Jane', 'Doe', 21); Insert into Users values(9, 'James', 'Dove', 22);

## Authors
INSERT INTO Authors (id, name) VALUES (1, 'J.R.R. Tolkien'); INSERT INTO Authors (id, name) VALUES (2, 'Jane Austen'); INSERT INTO Authors (id, name) VALUES (3, 'Douglas Adams');

## AllDatatypeTransformation
INSERT INTO `AllDatatypeTransformation` VALUES (
    'example1', 10, 'Some text 1', '2023-05-23',
    100, 123456, 12.34, 123.456,
    12345.67, '2023-05-23 12:34:56', '2023-05-23 12:34:56', '12:34:56',
    '2023', X'48656C6C6F', '1', 1,
    X'0102030405060708090A0B0C0D0E0F1011121314', B'1010101'
);

INSERT INTO `AllDatatypeTransformation` VALUES (
    'example2', 20, 'Some text 2', '2022-12-31',
    200, 987654, 23.45, 234.567,
    23456.78, '2022-12-31 23:59:59', '2022-12-31 23:59:59', '23:59:59',
    '2022', X'576F726C64', '2', 0,
    X'141312111009080706050403020100FFFFEFEDED', B'0101010'
);

INSERT INTO `AllDatatypeTransformation` VALUES (
    'example3', 30, 'Some text 3', '2024-01-01',
    300, 112233, 34.56, 345.678,
    34567.89, '2024-01-01 00:00:00', '2024-01-01 00:00:00', '00:00:00',
    '2024', X'536F6D6520626C6F62', '3', 1,
    X'00112233445566778899AABBCCDDEEFF00112233', B'1110001'
);

INSERT INTO `AllDatatypeTransformation` VALUES (
    'example4', 40, 'Some text 4', '2021-11-11',
    400, 223344, 45.67, 456.789,
    45678.90, '2021-11-11 11:11:11', '2021-11-11 11:11:11', '11:11:11',
    '2021', X'416E6F7468657220626C6F62', '1', 0,
    X'FFEEDDCCBBAA99887766554433221100AABBCCDD', B'1111111'
);

UPDATE `AllDatatypeTransformation` SET
    `tinyint_column` = 25,
    `text_column` = 'Updated text 2',
    `date_column` = '2023-01-01',
    `int_column` = 250,
    `bigint_column` = 56789,
    `float_column` = 25.45,
    `double_column` = 345.678,
    `decimal_column` = 23456.79,
    `datetime_column` = '2023-01-01 12:00:00',
    `timestamp_column` = '2023-01-01 12:00:00',
    `time_column` = '12:00:00',
    `year_column` = '2023',
    `blob_column` = X'1234567890ABCDEF',
    `enum_column` = '3',
    `bool_column` = 1,
    `binary_column` = X'1234567890ABCDEF1234567890ABCDEF12345678',
    `bit_column` = B'1111111'
WHERE `varchar_column` = 'example2';

UPDATE `AllDatatypeTransformation` SET
    `tinyint_column` = 35,
    `text_column` = 'Updated text 3',
    `date_column` = '2024-01-02',
    `int_column` = 350,
    `bigint_column` = 88000,
    `float_column` = 35.67,
    `double_column` = 456.789,
    `decimal_column` = 34567.90,
    `datetime_column` = '2024-01-02 00:00:00',
    `timestamp_column` = '2024-01-02 00:00:00',
    `time_column` = '01:00:00',
    `year_column` = '2025',
    `blob_column` = X'ABCDEF1234567890',
    `enum_column` = '1',
    `bool_column` = 0,
    `binary_column` = X'ABCDEF1234567890ABCDEF1234567890ABCDEF12',
    `bit_column` = B'0000000'
WHERE `varchar_column` = 'example3';

DELETE FROM AllDatatypeTransformation WHERE varchar_column = 'example4';


