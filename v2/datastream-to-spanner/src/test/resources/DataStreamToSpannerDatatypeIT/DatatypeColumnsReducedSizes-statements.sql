## Backfill events
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