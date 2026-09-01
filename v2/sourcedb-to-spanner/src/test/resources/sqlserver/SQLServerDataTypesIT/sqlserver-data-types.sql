-- SQL Server DataTypes test schema and sample data

-- ============================================================================
-- Scenario A: Default Type Migration
-- ============================================================================

CREATE TABLE tinyint_table (
    id INT PRIMARY KEY,
    tinyint_col TINYINT DEFAULT NULL
);
INSERT INTO tinyint_table (id, tinyint_col) VALUES (1, 0);
INSERT INTO tinyint_table (id, tinyint_col) VALUES (2, 255);
INSERT INTO tinyint_table (id, tinyint_col) VALUES (3, 10);
INSERT INTO tinyint_table (id, tinyint_col) VALUES (4, 127);
INSERT INTO tinyint_table (id, tinyint_col) VALUES (5, NULL);

CREATE TABLE smallint_table (
    id INT PRIMARY KEY,
    smallint_col SMALLINT DEFAULT NULL
);
INSERT INTO smallint_table (id, smallint_col) VALUES (1, -32768);
INSERT INTO smallint_table (id, smallint_col) VALUES (2, 32767);
INSERT INTO smallint_table (id, smallint_col) VALUES (3, 0);
INSERT INTO smallint_table (id, smallint_col) VALUES (4, 15);
INSERT INTO smallint_table (id, smallint_col) VALUES (5, NULL);

CREATE TABLE int_table (
    id INT PRIMARY KEY,
    int_col INT DEFAULT NULL
);
INSERT INTO int_table (id, int_col) VALUES (1, -2147483648);
INSERT INTO int_table (id, int_col) VALUES (2, 2147483647);
INSERT INTO int_table (id, int_col) VALUES (3, 0);
INSERT INTO int_table (id, int_col) VALUES (4, 30);
INSERT INTO int_table (id, int_col) VALUES (5, NULL);

CREATE TABLE bigint_table (
    id INT PRIMARY KEY,
    bigint_col BIGINT DEFAULT NULL
);
INSERT INTO bigint_table (id, bigint_col) VALUES (1, -9223372036854775808);
INSERT INTO bigint_table (id, bigint_col) VALUES (2, 9223372036854775807);
INSERT INTO bigint_table (id, bigint_col) VALUES (3, 0);
INSERT INTO bigint_table (id, bigint_col) VALUES (4, 40);
INSERT INTO bigint_table (id, bigint_col) VALUES (5, NULL);

CREATE TABLE bit_table (
    id INT PRIMARY KEY,
    bit_col BIT DEFAULT NULL
);
INSERT INTO bit_table (id, bit_col) VALUES (1, 0);
INSERT INTO bit_table (id, bit_col) VALUES (2, 1);
INSERT INTO bit_table (id, bit_col) VALUES (3, NULL);

CREATE TABLE decimal_table (
    id INT PRIMARY KEY,
    decimal_col DECIMAL(18, 4) DEFAULT NULL
);
INSERT INTO decimal_table (id, decimal_col) VALUES (1, -99999999.9999);
INSERT INTO decimal_table (id, decimal_col) VALUES (2, 99999999.9999);
INSERT INTO decimal_table (id, decimal_col) VALUES (3, 0.0000);
INSERT INTO decimal_table (id, decimal_col) VALUES (4, 12345.6789);
INSERT INTO decimal_table (id, decimal_col) VALUES (5, NULL);

CREATE TABLE numeric_table (
    id INT PRIMARY KEY,
    numeric_col NUMERIC(18, 4) DEFAULT NULL
);
INSERT INTO numeric_table (id, numeric_col) VALUES (1, -99999999.9999);
INSERT INTO numeric_table (id, numeric_col) VALUES (2, 99999999.9999);
INSERT INTO numeric_table (id, numeric_col) VALUES (3, 0.0000);
INSERT INTO numeric_table (id, numeric_col) VALUES (4, 12345.6789);
INSERT INTO numeric_table (id, numeric_col) VALUES (5, NULL);

CREATE TABLE money_table (
    id INT PRIMARY KEY,
    money_col MONEY DEFAULT NULL
);
INSERT INTO money_table (id, money_col) VALUES (1, -922337203685477.5808);
INSERT INTO money_table (id, money_col) VALUES (2, 922337203685477.5807);
INSERT INTO money_table (id, money_col) VALUES (3, 0.0000);
INSERT INTO money_table (id, money_col) VALUES (4, 123.4500);
INSERT INTO money_table (id, money_col) VALUES (5, NULL);

CREATE TABLE smallmoney_table (
    id INT PRIMARY KEY,
    smallmoney_col SMALLMONEY DEFAULT NULL
);
INSERT INTO smallmoney_table (id, smallmoney_col) VALUES (1, -214748.3648);
INSERT INTO smallmoney_table (id, smallmoney_col) VALUES (2, 214748.3647);
INSERT INTO smallmoney_table (id, smallmoney_col) VALUES (3, 0.0000);
INSERT INTO smallmoney_table (id, smallmoney_col) VALUES (4, 123.4500);
INSERT INTO smallmoney_table (id, smallmoney_col) VALUES (5, NULL);

CREATE TABLE float_table (
    id INT PRIMARY KEY,
    float_col FLOAT DEFAULT NULL
);
INSERT INTO float_table (id, float_col) VALUES (1, -1.79E+308);
INSERT INTO float_table (id, float_col) VALUES (2, 1.79E+308);
INSERT INTO float_table (id, float_col) VALUES (3, 0.0);
INSERT INTO float_table (id, float_col) VALUES (4, 45.56);
INSERT INTO float_table (id, float_col) VALUES (5, NULL);

CREATE TABLE real_table (
    id INT PRIMARY KEY,
    real_col REAL DEFAULT NULL
);
INSERT INTO real_table (id, real_col) VALUES (1, -3.40E+38);
INSERT INTO real_table (id, real_col) VALUES (2, 3.40E+38);
INSERT INTO real_table (id, real_col) VALUES (3, 0.0);
INSERT INTO real_table (id, real_col) VALUES (4, 45.56);
INSERT INTO real_table (id, real_col) VALUES (5, NULL);

CREATE TABLE date_table (
    id INT PRIMARY KEY,
    date_col DATE DEFAULT NULL
);
INSERT INTO date_table (id, date_col) VALUES (1, '0001-01-01');
INSERT INTO date_table (id, date_col) VALUES (2, '9999-12-31');
INSERT INTO date_table (id, date_col) VALUES (3, '2024-05-15');
INSERT INTO date_table (id, date_col) VALUES (4, NULL);

CREATE TABLE time_table (
    id INT PRIMARY KEY,
    time_col TIME DEFAULT NULL
);
INSERT INTO time_table (id, time_col) VALUES (1, '00:00:00.0000000');
INSERT INTO time_table (id, time_col) VALUES (2, '23:59:59.9999999');
INSERT INTO time_table (id, time_col) VALUES (3, '12:34:56.7890000');
INSERT INTO time_table (id, time_col) VALUES (4, NULL);

CREATE TABLE datetime2_table (
    id INT PRIMARY KEY,
    datetime2_col DATETIME2 DEFAULT NULL
);
INSERT INTO datetime2_table (id, datetime2_col) VALUES (1, '1970-01-01 00:00:00.0000000');
INSERT INTO datetime2_table (id, datetime2_col) VALUES (2, '2024-05-15 12:34:56.7890000');
INSERT INTO datetime2_table (id, datetime2_col) VALUES (3, '9999-12-31 23:59:59.0000000');
INSERT INTO datetime2_table (id, datetime2_col) VALUES (4, NULL);

CREATE TABLE datetimeoffset_table (
    id INT PRIMARY KEY,
    datetimeoffset_col DATETIMEOFFSET DEFAULT NULL
);
INSERT INTO datetimeoffset_table (id, datetimeoffset_col) VALUES (1, '1970-01-01 00:00:00.0000000 +00:00');
INSERT INTO datetimeoffset_table (id, datetimeoffset_col) VALUES (2, '2024-05-15 12:34:56.7890000 +00:00');
INSERT INTO datetimeoffset_table (id, datetimeoffset_col) VALUES (3, '9999-12-31 23:59:59.0000000 +00:00');
INSERT INTO datetimeoffset_table (id, datetimeoffset_col) VALUES (4, NULL);

CREATE TABLE datetime_table (
    id INT PRIMARY KEY,
    datetime_col DATETIME DEFAULT NULL
);
INSERT INTO datetime_table (id, datetime_col) VALUES (1, '1753-01-01 00:00:00.000');
INSERT INTO datetime_table (id, datetime_col) VALUES (2, '2024-05-15 12:34:56.000');
INSERT INTO datetime_table (id, datetime_col) VALUES (3, '9999-12-31 23:59:59.997');
INSERT INTO datetime_table (id, datetime_col) VALUES (4, NULL);

CREATE TABLE smalldatetime_table (
    id INT PRIMARY KEY,
    smalldatetime_col SMALLDATETIME DEFAULT NULL
);
INSERT INTO smalldatetime_table (id, smalldatetime_col) VALUES (1, '1900-01-01 00:00:00');
INSERT INTO smalldatetime_table (id, smalldatetime_col) VALUES (2, '2024-05-15 12:34:00');
INSERT INTO smalldatetime_table (id, smalldatetime_col) VALUES (3, '2079-06-06 23:59:00');
INSERT INTO smalldatetime_table (id, smalldatetime_col) VALUES (4, NULL);

CREATE TABLE char_table (
    id INT PRIMARY KEY,
    char_col CHAR(10) DEFAULT NULL
);
INSERT INTO char_table (id, char_col) VALUES (1, 'a');
INSERT INTO char_table (id, char_col) VALUES (2, 'test');
INSERT INTO char_table (id, char_col) VALUES (3, NULL);

CREATE TABLE varchar_table (
    id INT PRIMARY KEY,
    varchar_col VARCHAR(255) DEFAULT NULL
);
INSERT INTO varchar_table (id, varchar_col) VALUES (1, 'hello');
INSERT INTO varchar_table (id, varchar_col) VALUES (2, 'test varchar');
INSERT INTO varchar_table (id, varchar_col) VALUES (3, NULL);

CREATE TABLE varchar_max_table (
    id INT PRIMARY KEY,
    varchar_max_col VARCHAR(MAX) DEFAULT NULL
);
INSERT INTO varchar_max_table (id, varchar_max_col) VALUES (1, 'large varchar max payload content');
INSERT INTO varchar_max_table (id, varchar_max_col) VALUES (2, NULL);

CREATE TABLE text_table (
    id INT PRIMARY KEY,
    text_col TEXT DEFAULT NULL
);
INSERT INTO text_table (id, text_col) VALUES (1, 'sample text');
INSERT INTO text_table (id, text_col) VALUES (2, NULL);

CREATE TABLE nchar_table (
    id INT PRIMARY KEY,
    nchar_col NCHAR(10) DEFAULT NULL
);
INSERT INTO nchar_table (id, nchar_col) VALUES (1, N'a');
INSERT INTO nchar_table (id, nchar_col) VALUES (2, N'unicode');
INSERT INTO nchar_table (id, nchar_col) VALUES (3, NULL);

CREATE TABLE nvarchar_table (
    id INT PRIMARY KEY,
    nvarchar_col NVARCHAR(255) DEFAULT NULL
);
INSERT INTO nvarchar_table (id, nvarchar_col) VALUES (1, N'unicode test');
INSERT INTO nvarchar_table (id, nvarchar_col) VALUES (2, N'special chars');
INSERT INTO nvarchar_table (id, nvarchar_col) VALUES (3, NULL);

CREATE TABLE nvarchar_max_table (
    id INT PRIMARY KEY,
    nvarchar_max_col NVARCHAR(MAX) DEFAULT NULL
);
INSERT INTO nvarchar_max_table (id, nvarchar_max_col) VALUES (1, N'nvarchar max payload content');
INSERT INTO nvarchar_max_table (id, nvarchar_max_col) VALUES (2, NULL);

CREATE TABLE ntext_table (
    id INT PRIMARY KEY,
    ntext_col NTEXT DEFAULT NULL
);
INSERT INTO ntext_table (id, ntext_col) VALUES (1, N'sample ntext');
INSERT INTO ntext_table (id, ntext_col) VALUES (2, NULL);

CREATE TABLE binary_table (
    id INT PRIMARY KEY,
    binary_col BINARY(4) DEFAULT NULL
);
INSERT INTO binary_table (id, binary_col) VALUES (1, 0x00000000);
INSERT INTO binary_table (id, binary_col) VALUES (2, 0x12345678);
INSERT INTO binary_table (id, binary_col) VALUES (3, 0xFFFFFFFF);
INSERT INTO binary_table (id, binary_col) VALUES (4, NULL);

CREATE TABLE varbinary_table (
    id INT PRIMARY KEY,
    varbinary_col VARBINARY(255) DEFAULT NULL
);
INSERT INTO varbinary_table (id, varbinary_col) VALUES (1, 0x00);
INSERT INTO varbinary_table (id, varbinary_col) VALUES (2, 0x123456);
INSERT INTO varbinary_table (id, varbinary_col) VALUES (3, 0xFF);
INSERT INTO varbinary_table (id, varbinary_col) VALUES (4, NULL);

CREATE TABLE varbinary_max_table (
    id INT PRIMARY KEY,
    varbinary_max_col VARBINARY(MAX) DEFAULT NULL
);
INSERT INTO varbinary_max_table (id, varbinary_max_col) VALUES (1, 0x0102030405);
INSERT INTO varbinary_max_table (id, varbinary_max_col) VALUES (2, NULL);

CREATE TABLE image_table (
    id INT PRIMARY KEY,
    image_col IMAGE DEFAULT NULL
);
INSERT INTO image_table (id, image_col) VALUES (1, 0x00);
INSERT INTO image_table (id, image_col) VALUES (2, 0x123456);
INSERT INTO image_table (id, image_col) VALUES (3, 0xFF);
INSERT INTO image_table (id, image_col) VALUES (4, NULL);

CREATE TABLE uniqueidentifier_table (
    id INT PRIMARY KEY,
    uniqueidentifier_col UNIQUEIDENTIFIER DEFAULT NULL
);
INSERT INTO uniqueidentifier_table (id, uniqueidentifier_col) VALUES (1, '6F9619FF-8B86-D011-B42D-00C04FC964FF');
INSERT INTO uniqueidentifier_table (id, uniqueidentifier_col) VALUES (2, 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11');
INSERT INTO uniqueidentifier_table (id, uniqueidentifier_col) VALUES (3, NULL);

CREATE TABLE xml_table (
    id INT PRIMARY KEY,
    xml_col XML DEFAULT NULL
);
INSERT INTO xml_table (id, xml_col) VALUES (1, '<root><child>value</child></root>');
INSERT INTO xml_table (id, xml_col) VALUES (2, '<item id="1">text</item>');
INSERT INTO xml_table (id, xml_col) VALUES (3, NULL);


-- ============================================================================
-- Scenario B: Alternative Type Migration
-- ============================================================================

CREATE TABLE tinyint_to_string_table (
    id INT PRIMARY KEY,
    tinyint_to_string_col TINYINT DEFAULT NULL
);
INSERT INTO tinyint_to_string_table (id, tinyint_to_string_col) VALUES (1, 0);
INSERT INTO tinyint_to_string_table (id, tinyint_to_string_col) VALUES (2, 255);
INSERT INTO tinyint_to_string_table (id, tinyint_to_string_col) VALUES (3, 10);
INSERT INTO tinyint_to_string_table (id, tinyint_to_string_col) VALUES (4, 127);
INSERT INTO tinyint_to_string_table (id, tinyint_to_string_col) VALUES (5, NULL);

CREATE TABLE smallint_to_string_table (
    id INT PRIMARY KEY,
    smallint_to_string_col SMALLINT DEFAULT NULL
);
INSERT INTO smallint_to_string_table (id, smallint_to_string_col) VALUES (1, -32768);
INSERT INTO smallint_to_string_table (id, smallint_to_string_col) VALUES (2, 32767);
INSERT INTO smallint_to_string_table (id, smallint_to_string_col) VALUES (3, 0);
INSERT INTO smallint_to_string_table (id, smallint_to_string_col) VALUES (4, 15);
INSERT INTO smallint_to_string_table (id, smallint_to_string_col) VALUES (5, NULL);

CREATE TABLE int_to_string_table (
    id INT PRIMARY KEY,
    int_to_string_col INT DEFAULT NULL
);
INSERT INTO int_to_string_table (id, int_to_string_col) VALUES (1, -2147483648);
INSERT INTO int_to_string_table (id, int_to_string_col) VALUES (2, 2147483647);
INSERT INTO int_to_string_table (id, int_to_string_col) VALUES (3, 0);
INSERT INTO int_to_string_table (id, int_to_string_col) VALUES (4, 30);
INSERT INTO int_to_string_table (id, int_to_string_col) VALUES (5, NULL);

CREATE TABLE bigint_to_string_table (
    id INT PRIMARY KEY,
    bigint_to_string_col BIGINT DEFAULT NULL
);
INSERT INTO bigint_to_string_table (id, bigint_to_string_col) VALUES (1, -9223372036854775808);
INSERT INTO bigint_to_string_table (id, bigint_to_string_col) VALUES (2, 9223372036854775807);
INSERT INTO bigint_to_string_table (id, bigint_to_string_col) VALUES (3, 0);
INSERT INTO bigint_to_string_table (id, bigint_to_string_col) VALUES (4, 40);
INSERT INTO bigint_to_string_table (id, bigint_to_string_col) VALUES (5, NULL);

CREATE TABLE bit_to_int64_table (
    id INT PRIMARY KEY,
    bit_to_int64_col BIT DEFAULT NULL
);
INSERT INTO bit_to_int64_table (id, bit_to_int64_col) VALUES (1, 0);
INSERT INTO bit_to_int64_table (id, bit_to_int64_col) VALUES (2, 1);
INSERT INTO bit_to_int64_table (id, bit_to_int64_col) VALUES (3, NULL);

CREATE TABLE decimal_to_float64_table (
    id INT PRIMARY KEY,
    decimal_to_float64_col DECIMAL(18, 4) DEFAULT NULL
);
INSERT INTO decimal_to_float64_table (id, decimal_to_float64_col) VALUES (1, -99999999.9999);
INSERT INTO decimal_to_float64_table (id, decimal_to_float64_col) VALUES (2, 99999999.9999);
INSERT INTO decimal_to_float64_table (id, decimal_to_float64_col) VALUES (3, 0.0000);
INSERT INTO decimal_to_float64_table (id, decimal_to_float64_col) VALUES (4, 12345.6789);
INSERT INTO decimal_to_float64_table (id, decimal_to_float64_col) VALUES (5, NULL);

CREATE TABLE decimal_to_string_table (
    id INT PRIMARY KEY,
    decimal_to_string_col DECIMAL(18, 4) DEFAULT NULL
);
INSERT INTO decimal_to_string_table (id, decimal_to_string_col) VALUES (1, -99999999.9999);
INSERT INTO decimal_to_string_table (id, decimal_to_string_col) VALUES (2, 99999999.9999);
INSERT INTO decimal_to_string_table (id, decimal_to_string_col) VALUES (3, 0.0000);
INSERT INTO decimal_to_string_table (id, decimal_to_string_col) VALUES (4, 12345.6789);
INSERT INTO decimal_to_string_table (id, decimal_to_string_col) VALUES (5, NULL);

CREATE TABLE numeric_to_float64_table (
    id INT PRIMARY KEY,
    numeric_to_float64_col NUMERIC(18, 4) DEFAULT NULL
);
INSERT INTO numeric_to_float64_table (id, numeric_to_float64_col) VALUES (1, -99999999.9999);
INSERT INTO numeric_to_float64_table (id, numeric_to_float64_col) VALUES (2, 99999999.9999);
INSERT INTO numeric_to_float64_table (id, numeric_to_float64_col) VALUES (3, 0.0000);
INSERT INTO numeric_to_float64_table (id, numeric_to_float64_col) VALUES (4, 12345.6789);
INSERT INTO numeric_to_float64_table (id, numeric_to_float64_col) VALUES (5, NULL);

CREATE TABLE numeric_to_string_table (
    id INT PRIMARY KEY,
    numeric_to_string_col NUMERIC(18, 4) DEFAULT NULL
);
INSERT INTO numeric_to_string_table (id, numeric_to_string_col) VALUES (1, -99999999.9999);
INSERT INTO numeric_to_string_table (id, numeric_to_string_col) VALUES (2, 99999999.9999);
INSERT INTO numeric_to_string_table (id, numeric_to_string_col) VALUES (3, 0.0000);
INSERT INTO numeric_to_string_table (id, numeric_to_string_col) VALUES (4, 12345.6789);
INSERT INTO numeric_to_string_table (id, numeric_to_string_col) VALUES (5, NULL);

CREATE TABLE money_to_float64_table (
    id INT PRIMARY KEY,
    money_to_float64_col MONEY DEFAULT NULL
);
INSERT INTO money_to_float64_table (id, money_to_float64_col) VALUES (1, -922337203685477.5808);
INSERT INTO money_to_float64_table (id, money_to_float64_col) VALUES (2, 922337203685477.5807);
INSERT INTO money_to_float64_table (id, money_to_float64_col) VALUES (3, 0.0000);
INSERT INTO money_to_float64_table (id, money_to_float64_col) VALUES (4, 123.4500);
INSERT INTO money_to_float64_table (id, money_to_float64_col) VALUES (5, NULL);

CREATE TABLE money_to_string_table (
    id INT PRIMARY KEY,
    money_to_string_col MONEY DEFAULT NULL
);
INSERT INTO money_to_string_table (id, money_to_string_col) VALUES (1, -922337203685477.5808);
INSERT INTO money_to_string_table (id, money_to_string_col) VALUES (2, 922337203685477.5807);
INSERT INTO money_to_string_table (id, money_to_string_col) VALUES (3, 0.0000);
INSERT INTO money_to_string_table (id, money_to_string_col) VALUES (4, 123.4500);
INSERT INTO money_to_string_table (id, money_to_string_col) VALUES (5, NULL);

CREATE TABLE smallmoney_to_float64_table (
    id INT PRIMARY KEY,
    smallmoney_to_float64_col SMALLMONEY DEFAULT NULL
);
INSERT INTO smallmoney_to_float64_table (id, smallmoney_to_float64_col) VALUES (1, -214748.3648);
INSERT INTO smallmoney_to_float64_table (id, smallmoney_to_float64_col) VALUES (2, 214748.3647);
INSERT INTO smallmoney_to_float64_table (id, smallmoney_to_float64_col) VALUES (3, 0.0000);
INSERT INTO smallmoney_to_float64_table (id, smallmoney_to_float64_col) VALUES (4, 123.4500);
INSERT INTO smallmoney_to_float64_table (id, smallmoney_to_float64_col) VALUES (5, NULL);

CREATE TABLE smallmoney_to_string_table (
    id INT PRIMARY KEY,
    smallmoney_to_string_col SMALLMONEY DEFAULT NULL
);
INSERT INTO smallmoney_to_string_table (id, smallmoney_to_string_col) VALUES (1, -214748.3648);
INSERT INTO smallmoney_to_string_table (id, smallmoney_to_string_col) VALUES (2, 214748.3647);
INSERT INTO smallmoney_to_string_table (id, smallmoney_to_string_col) VALUES (3, 0.0000);
INSERT INTO smallmoney_to_string_table (id, smallmoney_to_string_col) VALUES (4, 123.4500);
INSERT INTO smallmoney_to_string_table (id, smallmoney_to_string_col) VALUES (5, NULL);

CREATE TABLE float_to_string_table (
    id INT PRIMARY KEY,
    float_to_string_col FLOAT DEFAULT NULL
);
INSERT INTO float_to_string_table (id, float_to_string_col) VALUES (1, -1.79E+308);
INSERT INTO float_to_string_table (id, float_to_string_col) VALUES (2, 1.79E+308);
INSERT INTO float_to_string_table (id, float_to_string_col) VALUES (3, 0.0);
INSERT INTO float_to_string_table (id, float_to_string_col) VALUES (4, 45.56);
INSERT INTO float_to_string_table (id, float_to_string_col) VALUES (5, NULL);

CREATE TABLE real_to_float64_table (
    id INT PRIMARY KEY,
    real_to_float64_col REAL DEFAULT NULL
);
INSERT INTO real_to_float64_table (id, real_to_float64_col) VALUES (1, -3.40E+38);
INSERT INTO real_to_float64_table (id, real_to_float64_col) VALUES (2, 3.40E+38);
INSERT INTO real_to_float64_table (id, real_to_float64_col) VALUES (3, 0.0);
INSERT INTO real_to_float64_table (id, real_to_float64_col) VALUES (4, 45.56);
INSERT INTO real_to_float64_table (id, real_to_float64_col) VALUES (5, NULL);

CREATE TABLE real_to_string_table (
    id INT PRIMARY KEY,
    real_to_string_col REAL DEFAULT NULL
);
INSERT INTO real_to_string_table (id, real_to_string_col) VALUES (1, -3.40E+38);
INSERT INTO real_to_string_table (id, real_to_string_col) VALUES (2, 3.40E+38);
INSERT INTO real_to_string_table (id, real_to_string_col) VALUES (3, 0.0);
INSERT INTO real_to_string_table (id, real_to_string_col) VALUES (4, 45.56);
INSERT INTO real_to_string_table (id, real_to_string_col) VALUES (5, NULL);

CREATE TABLE date_to_string_table (
    id INT PRIMARY KEY,
    date_to_string_col DATE DEFAULT NULL
);
INSERT INTO date_to_string_table (id, date_to_string_col) VALUES (1, '0001-01-01');
INSERT INTO date_to_string_table (id, date_to_string_col) VALUES (2, '9999-12-31');
INSERT INTO date_to_string_table (id, date_to_string_col) VALUES (3, '2024-05-15');
INSERT INTO date_to_string_table (id, date_to_string_col) VALUES (4, NULL);

CREATE TABLE datetime2_to_string_table (
    id INT PRIMARY KEY,
    datetime2_to_string_col DATETIME2 DEFAULT NULL
);
INSERT INTO datetime2_to_string_table (id, datetime2_to_string_col) VALUES (1, '1970-01-01 00:00:00.0000000');
INSERT INTO datetime2_to_string_table (id, datetime2_to_string_col) VALUES (2, '2024-05-15 12:34:56.7890000');
INSERT INTO datetime2_to_string_table (id, datetime2_to_string_col) VALUES (3, '9999-12-31 23:59:59.0000000');
INSERT INTO datetime2_to_string_table (id, datetime2_to_string_col) VALUES (4, NULL);

CREATE TABLE datetime_to_string_table (
    id INT PRIMARY KEY,
    datetime_to_string_col DATETIME DEFAULT NULL
);
INSERT INTO datetime_to_string_table (id, datetime_to_string_col) VALUES (1, '1753-01-01 00:00:00.000');
INSERT INTO datetime_to_string_table (id, datetime_to_string_col) VALUES (2, '2024-05-15 12:34:56.000');
INSERT INTO datetime_to_string_table (id, datetime_to_string_col) VALUES (3, '9999-12-31 23:59:59.997');
INSERT INTO datetime_to_string_table (id, datetime_to_string_col) VALUES (4, NULL);

CREATE TABLE smalldatetime_to_string_table (
    id INT PRIMARY KEY,
    smalldatetime_to_string_col SMALLDATETIME DEFAULT NULL
);
INSERT INTO smalldatetime_to_string_table (id, smalldatetime_to_string_col) VALUES (1, '1900-01-01 00:00:00');
INSERT INTO smalldatetime_to_string_table (id, smalldatetime_to_string_col) VALUES (2, '2024-05-15 12:34:00');
INSERT INTO smalldatetime_to_string_table (id, smalldatetime_to_string_col) VALUES (3, '2079-06-06 23:59:00');
INSERT INTO smalldatetime_to_string_table (id, smalldatetime_to_string_col) VALUES (4, NULL);

CREATE TABLE binary_to_string_table (
    id INT PRIMARY KEY,
    binary_to_string_col BINARY(4) DEFAULT NULL
);
INSERT INTO binary_to_string_table (id, binary_to_string_col) VALUES (1, 0x00000000);
INSERT INTO binary_to_string_table (id, binary_to_string_col) VALUES (2, 0x12345678);
INSERT INTO binary_to_string_table (id, binary_to_string_col) VALUES (3, 0xFFFFFFFF);
INSERT INTO binary_to_string_table (id, binary_to_string_col) VALUES (4, NULL);

CREATE TABLE varbinary_to_string_table (
    id INT PRIMARY KEY,
    varbinary_to_string_col VARBINARY(255) DEFAULT NULL
);
INSERT INTO varbinary_to_string_table (id, varbinary_to_string_col) VALUES (1, 0x00);
INSERT INTO varbinary_to_string_table (id, varbinary_to_string_col) VALUES (2, 0x123456);
INSERT INTO varbinary_to_string_table (id, varbinary_to_string_col) VALUES (3, 0xFF);
INSERT INTO varbinary_to_string_table (id, varbinary_to_string_col) VALUES (4, NULL);


-- ============================================================================
-- Scenario C: Primary Key Mapping
-- ============================================================================

CREATE TABLE tinyint_pk_table (
    id TINYINT PRIMARY KEY,
    val VARCHAR(50) DEFAULT NULL
);
INSERT INTO tinyint_pk_table (id, val) VALUES (0, 'zero');
INSERT INTO tinyint_pk_table (id, val) VALUES (127, 'mid');
INSERT INTO tinyint_pk_table (id, val) VALUES (255, 'max');

CREATE TABLE smallint_pk_table (
    id SMALLINT PRIMARY KEY,
    val VARCHAR(50) DEFAULT NULL
);
INSERT INTO smallint_pk_table (id, val) VALUES (-32768, 'min');
INSERT INTO smallint_pk_table (id, val) VALUES (0, 'zero');
INSERT INTO smallint_pk_table (id, val) VALUES (32767, 'max');

CREATE TABLE int_pk_table (
    id INT PRIMARY KEY,
    val VARCHAR(50) DEFAULT NULL
);
INSERT INTO int_pk_table (id, val) VALUES (-2147483648, 'min');
INSERT INTO int_pk_table (id, val) VALUES (0, 'zero');
INSERT INTO int_pk_table (id, val) VALUES (2147483647, 'max');

CREATE TABLE bigint_pk_table (
    id BIGINT PRIMARY KEY,
    val VARCHAR(50) DEFAULT NULL
);
INSERT INTO bigint_pk_table (id, val) VALUES (-9223372036854775808, 'min');
INSERT INTO bigint_pk_table (id, val) VALUES (0, 'zero');
INSERT INTO bigint_pk_table (id, val) VALUES (9223372036854775807, 'max');

CREATE TABLE bit_pk_table (
    id BIT PRIMARY KEY,
    val VARCHAR(50) DEFAULT NULL
);
INSERT INTO bit_pk_table (id, val) VALUES (0, 'false');
INSERT INTO bit_pk_table (id, val) VALUES (1, 'true');

CREATE TABLE date_pk_table (
    id DATE PRIMARY KEY,
    val VARCHAR(50) DEFAULT NULL
);
INSERT INTO date_pk_table (id, val) VALUES ('1000-01-01', 'ancient');
INSERT INTO date_pk_table (id, val) VALUES ('2024-05-15', 'current');
INSERT INTO date_pk_table (id, val) VALUES ('9999-12-31', 'max');

CREATE TABLE char_pk_table (
    id CHAR(10) PRIMARY KEY,
    val VARCHAR(50) DEFAULT NULL
);
INSERT INTO char_pk_table (id, val) VALUES ('pk1', 'val1');
INSERT INTO char_pk_table (id, val) VALUES ('pk2', 'val2');

CREATE TABLE varchar_pk_table (
    id VARCHAR(50) PRIMARY KEY,
    val VARCHAR(50) DEFAULT NULL
);
INSERT INTO varchar_pk_table (id, val) VALUES ('key1', 'val1');
INSERT INTO varchar_pk_table (id, val) VALUES ('key2', 'val2');

CREATE TABLE nchar_pk_table (
    id NCHAR(10) PRIMARY KEY,
    val VARCHAR(50) DEFAULT NULL
);
INSERT INTO nchar_pk_table (id, val) VALUES (N'npk1', 'val1');
INSERT INTO nchar_pk_table (id, val) VALUES (N'npk2', 'val2');

CREATE TABLE nvarchar_pk_table (
    id NVARCHAR(50) PRIMARY KEY,
    val VARCHAR(50) DEFAULT NULL
);
INSERT INTO nvarchar_pk_table (id, val) VALUES (N'nkey1', 'val1');
INSERT INTO nvarchar_pk_table (id, val) VALUES (N'nkey2', 'val2');

CREATE TABLE binary_pk_table (
    id BINARY(4) PRIMARY KEY,
    val VARCHAR(50) DEFAULT NULL
);
INSERT INTO binary_pk_table (id, val) VALUES (0x00000001, 'b1');
INSERT INTO binary_pk_table (id, val) VALUES (0x00000002, 'b2');

CREATE TABLE varbinary_pk_table (
    id VARBINARY(50) PRIMARY KEY,
    val VARCHAR(50) DEFAULT NULL
);
INSERT INTO varbinary_pk_table (id, val) VALUES (0x0102, 'vb1');
INSERT INTO varbinary_pk_table (id, val) VALUES (0x0304, 'vb2');

CREATE TABLE uniqueidentifier_pk_table (
    id UNIQUEIDENTIFIER PRIMARY KEY,
    val VARCHAR(50) DEFAULT NULL
);
INSERT INTO uniqueidentifier_pk_table (id, val) VALUES ('6F9619FF-8B86-D011-B42D-00C04FC964FF', 'u1');
INSERT INTO uniqueidentifier_pk_table (id, val) VALUES ('A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 'u2');


-- ============================================================================
-- Scenario D: Unsupported / Complex Types
-- ============================================================================

CREATE TABLE geography_table (
    id INT PRIMARY KEY,
    geography_col GEOGRAPHY DEFAULT NULL
);
INSERT INTO geography_table (id, geography_col) VALUES (1, geography::STGeomFromText('POINT(-122.34900 47.65100)', 4326));

CREATE TABLE geometry_table (
    id INT PRIMARY KEY,
    geometry_col GEOMETRY DEFAULT NULL
);
INSERT INTO geometry_table (id, geometry_col) VALUES (1, geometry::STGeomFromText('POINT(1 2)', 0));

CREATE TABLE hierarchyid_table (
    id INT PRIMARY KEY,
    hierarchyid_col HIERARCHYID DEFAULT NULL
);
INSERT INTO hierarchyid_table (id, hierarchyid_col) VALUES (1, hierarchyid::GetRoot());

CREATE TABLE sql_variant_table (
    id INT PRIMARY KEY,
    sql_variant_col SQL_VARIANT DEFAULT NULL
);
INSERT INTO sql_variant_table (id, sql_variant_col) VALUES (1, CAST(12345 AS sql_variant));
