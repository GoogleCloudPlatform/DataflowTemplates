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