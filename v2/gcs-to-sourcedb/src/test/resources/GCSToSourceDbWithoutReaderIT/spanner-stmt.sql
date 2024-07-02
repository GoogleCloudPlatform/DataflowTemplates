INSERT INTO Users (id, name) VALUES (1, 'FF');

INSERT INTO AllDatatypeTransformation (
    varchar_column, bigint_column, binary_column, bit_column, blob_column, bool_column,
    date_column, datetime_column, decimal_column, double_column, enum_column, float_column,
    int_column, text_column, time_column, timestamp_column, tinyint_column, year_column
) VALUES (
    'example2', 1000, b'bin_column', b'1',
    b'blob_column', true, '2024-01-01', '2024-01-01T12:34:56Z', 99999.99,
    123456.123, '1', 12345.67, 100, 'Sample text for entry 1', '410000',
    '2024-01-01T12:34:56Z', 1, '2024'
);

update AllDatatypeTransformation set tinyint_column=2 where varchar_column='example2';

