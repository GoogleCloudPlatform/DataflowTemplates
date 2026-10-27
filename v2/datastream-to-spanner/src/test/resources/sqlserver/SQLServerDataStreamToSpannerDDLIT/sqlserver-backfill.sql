INSERT INTO AllDatatypeTransformation (varchar_column, tinyint_column, text_column, date_column, int_column, bigint_column, float_column, double_column, decimal_column, datetime_column, timestamp_column, time_column, year_column, blob_column, enum_column, bool_column, binary_column, bit_column) VALUES ('example1', 10, 'Some text 1', '2022-01-01', 100, 123456, 12.34, 123.456, 12345.67, '2022-01-01 00:00:00 +00:00', '2022-01-01 00:00:00 +00:00', '10:10:10', 2021, 0x1234, '1', 1, 0x1234, 1);

INSERT INTO AllDatatypeTransformation (varchar_column, tinyint_column, text_column, date_column, int_column, bigint_column, float_column, double_column, decimal_column, datetime_column, timestamp_column, time_column, year_column, blob_column, enum_column, bool_column, binary_column, bit_column) VALUES ('example2', 20, 'Some text 2', '2022-12-31', 200, 987654, 23.45, 234.567, 23456.78, '2022-12-31 23:59:59 +00:00', '2022-12-31 23:59:59 +00:00', '23:59:59', 2022, 0x1234, '2', 0, 0x1234, 2);

INSERT INTO AllDatatypeTransformation (varchar_column, tinyint_column, text_column, date_column, int_column, bigint_column, float_column, double_column, decimal_column, datetime_column, timestamp_column, time_column, year_column, blob_column, enum_column, bool_column, binary_column, bit_column) VALUES ('example3', 30, 'Some text 3', '2024-01-01', 300, 112233, 34.56, 345.678, 34567.89, '2024-01-01 00:00:00 +00:00', '2024-01-01 00:00:00 +00:00', '00:00:00', 2024, 0x1234, '3', 0, 0x1234, 3);

INSERT INTO AllDatatypeTransformation (varchar_column, tinyint_column, text_column, date_column, int_column, bigint_column, float_column, double_column, decimal_column, datetime_column, timestamp_column, time_column, year_column, blob_column, enum_column, bool_column, binary_column, bit_column) VALUES ('example4', 40, 'Some text 4', '2021-11-11', 400, 223344, 45.67, 456.789, 45678.90, '2021-11-11 11:11:11 +00:00', '2021-11-11 11:11:11 +00:00', '11:11:11', 2021, 0x1234, '2', 1, 0x1234, 4);

INSERT INTO DatatypeColumnsWithSizes (varchar_column, float_column, decimal_column, char_column, nvarchar_column, bool_column, binary_column, varbinary_column, bit_column) VALUES ('value1', 45.67, 456.12, 'char_1', N'nvarchar_1', 0, 0x62696E6172795F31, 0x76617262696E6172795F31, 102);

INSERT INTO DatatypeColumnsWithSizes (varchar_column, float_column, decimal_column, char_column, nvarchar_column, bool_column, binary_column, varbinary_column, bit_column) VALUES ('value2', 12.34, 123.45, 'char_2', N'nvarchar_2', 1, 0x62696E6172795F32, 0x76617262696E6172795F32, 25);

INSERT INTO Books (title) VALUES ('The Lord of the Rings');

INSERT INTO Books (title) VALUES ('Pride and Prejudice');

INSERT INTO Books (title) VALUES ('The Hitchhiker''s Guide to the Galaxy');
