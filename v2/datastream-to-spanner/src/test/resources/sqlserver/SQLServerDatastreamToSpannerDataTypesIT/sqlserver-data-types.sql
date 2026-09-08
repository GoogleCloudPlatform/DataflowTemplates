CREATE TABLE varchar_table (
    id INT PRIMARY KEY,
    varchar_col VARCHAR(8000) DEFAULT NULL
);

CREATE TABLE tinyint_table (
    id INT PRIMARY KEY,
    tinyint_col TINYINT DEFAULT NULL
);

CREATE TABLE smallint_table (
    id INT PRIMARY KEY,
    smallint_col SMALLINT DEFAULT NULL
);

CREATE TABLE int_table (
    id INT PRIMARY KEY,
    int_col INT DEFAULT NULL
);

CREATE TABLE bigint_table (
    id BIGINT PRIMARY KEY,
    bigint_col BIGINT DEFAULT NULL
);

CREATE TABLE bit_table (
    id INT PRIMARY KEY,
    bit_col BIT DEFAULT NULL
);

CREATE TABLE decimal_table (
    id INT PRIMARY KEY,
    decimal_col DECIMAL(38,9) DEFAULT NULL
);

CREATE TABLE numeric_table (
    id INT PRIMARY KEY,
    numeric_col NUMERIC(38,9) DEFAULT NULL
);

CREATE TABLE money_table (
    id INT PRIMARY KEY,
    money_col MONEY DEFAULT NULL
);

CREATE TABLE smallmoney_table (
    id INT PRIMARY KEY,
    smallmoney_col SMALLMONEY DEFAULT NULL
);

CREATE TABLE float_table (
    id INT PRIMARY KEY,
    float_col FLOAT(53) DEFAULT NULL
);

CREATE TABLE real_table (
    id INT PRIMARY KEY,
    real_col REAL DEFAULT NULL
);

CREATE TABLE date_table (
    id INT PRIMARY KEY,
    date_col DATE DEFAULT NULL
);

CREATE TABLE time_table (
    id INT PRIMARY KEY,
    time_col TIME DEFAULT NULL
);

CREATE TABLE datetime2_table (
    id INT PRIMARY KEY,
    datetime2_col DATETIME2 DEFAULT NULL
);

CREATE TABLE datetimeoffset_table (
    id INT PRIMARY KEY,
    datetimeoffset_col DATETIMEOFFSET DEFAULT NULL
);

CREATE TABLE datetime_table (
    id INT PRIMARY KEY,
    datetime_col DATETIME DEFAULT NULL
);

CREATE TABLE smalldatetime_table (
    id INT PRIMARY KEY,
    smalldatetime_col SMALLDATETIME DEFAULT NULL
);

CREATE TABLE char_table (
    id INT PRIMARY KEY,
    char_col CHAR(255) DEFAULT NULL
);

CREATE TABLE text_table (
    id INT PRIMARY KEY,
    text_col TEXT DEFAULT NULL
);

CREATE TABLE nchar_table (
    id INT PRIMARY KEY,
    nchar_col NCHAR(255) DEFAULT NULL
);

CREATE TABLE nvarchar_table (
    id INT PRIMARY KEY,
    nvarchar_col NVARCHAR(4000) DEFAULT NULL
);

CREATE TABLE ntext_table (
    id INT PRIMARY KEY,
    ntext_col NTEXT DEFAULT NULL
);

CREATE TABLE binary_table (
    id INT PRIMARY KEY,
    binary_col BINARY(255) DEFAULT NULL
);

CREATE TABLE varbinary_table (
    id INT PRIMARY KEY,
    varbinary_col VARBINARY(8000) DEFAULT NULL
);

CREATE TABLE image_table (
    id INT PRIMARY KEY,
    image_col IMAGE DEFAULT NULL
);

CREATE TABLE uniqueidentifier_table (
    id INT PRIMARY KEY,
    uniqueidentifier_col UNIQUEIDENTIFIER DEFAULT NULL
);

CREATE TABLE xml_table (
    id INT PRIMARY KEY,
    xml_col XML DEFAULT NULL
);

CREATE TABLE tinyint_to_string_table (
    id INT PRIMARY KEY,
    tinyint_to_string_col TINYINT DEFAULT NULL
);

CREATE TABLE smallint_to_string_table (
    id INT PRIMARY KEY,
    smallint_to_string_col SMALLINT DEFAULT NULL
);

CREATE TABLE int_to_string_table (
    id INT PRIMARY KEY,
    int_to_string_col INT DEFAULT NULL
);

CREATE TABLE bigint_to_string_table (
    id BIGINT PRIMARY KEY,
    bigint_to_string_col BIGINT DEFAULT NULL
);

CREATE TABLE bit_to_int64_table (
    id INT PRIMARY KEY,
    bit_to_int64_col BIT DEFAULT NULL
);

CREATE TABLE bit_to_string_table (
    id INT PRIMARY KEY,
    bit_to_string_col BIT DEFAULT NULL
);

CREATE TABLE decimal_to_string_table (
    id INT PRIMARY KEY,
    decimal_to_string_col DECIMAL(38,9) DEFAULT NULL
);

CREATE TABLE decimal_to_float64_table (
    id INT PRIMARY KEY,
    decimal_to_float64_col DECIMAL(38,9) DEFAULT NULL
);

CREATE TABLE numeric_to_string_table (
    id INT PRIMARY KEY,
    numeric_to_string_col NUMERIC(38,9) DEFAULT NULL
);

CREATE TABLE money_to_string_table (
    id INT PRIMARY KEY,
    money_to_string_col MONEY DEFAULT NULL
);

CREATE TABLE smallmoney_to_string_table (
    id INT PRIMARY KEY,
    smallmoney_to_string_col SMALLMONEY DEFAULT NULL
);

CREATE TABLE float_to_string_table (
    id INT PRIMARY KEY,
    float_to_string_col FLOAT(53) DEFAULT NULL
);

CREATE TABLE real_to_float64_table (
    id INT PRIMARY KEY,
    real_to_float64_col REAL DEFAULT NULL
);

CREATE TABLE real_to_string_table (
    id INT PRIMARY KEY,
    real_to_string_col REAL DEFAULT NULL
);

CREATE TABLE date_to_string_table (
    id INT PRIMARY KEY,
    date_to_string_col DATE DEFAULT NULL
);

CREATE TABLE datetime2_to_string_table (
    id INT PRIMARY KEY,
    datetime2_to_string_col DATETIME2 DEFAULT NULL
);

CREATE TABLE datetimeoffset_to_string_table (
    id INT PRIMARY KEY,
    datetimeoffset_to_string_col DATETIMEOFFSET DEFAULT NULL
);

CREATE TABLE datetime_to_string_table (
    id INT PRIMARY KEY,
    datetime_to_string_col DATETIME DEFAULT NULL
);

CREATE TABLE smalldatetime_to_string_table (
    id INT PRIMARY KEY,
    smalldatetime_to_string_col SMALLDATETIME DEFAULT NULL
);

CREATE TABLE char_to_bytes_table (
    id INT PRIMARY KEY,
    char_to_bytes_col CHAR(255) DEFAULT NULL
);

CREATE TABLE varchar_to_bytes_table (
    id INT PRIMARY KEY,
    varchar_to_bytes_col VARCHAR(8000) DEFAULT NULL
);

CREATE TABLE nchar_to_bytes_table (
    id INT PRIMARY KEY,
    nchar_to_bytes_col NCHAR(255) DEFAULT NULL
);

CREATE TABLE nvarchar_to_bytes_table (
    id INT PRIMARY KEY,
    nvarchar_to_bytes_col NVARCHAR(4000) DEFAULT NULL
);

CREATE TABLE binary_to_string_table (
    id INT PRIMARY KEY,
    binary_to_string_col BINARY(255) DEFAULT NULL
);

CREATE TABLE varbinary_to_string_table (
    id INT PRIMARY KEY,
    varbinary_to_string_col VARBINARY(8000) DEFAULT NULL
);

CREATE TABLE image_to_string_table (
    id INT PRIMARY KEY,
    image_to_string_col IMAGE DEFAULT NULL
);

CREATE TABLE tinyint_pk_table (
    id TINYINT PRIMARY KEY,
    tinyint_pk_col TINYINT DEFAULT NULL
);

CREATE TABLE smallint_pk_table (
    id SMALLINT PRIMARY KEY,
    smallint_pk_col SMALLINT DEFAULT NULL
);

CREATE TABLE int_pk_table (
    id INT PRIMARY KEY,
    int_pk_col INT DEFAULT NULL
);

CREATE TABLE bigint_pk_table (
    id BIGINT PRIMARY KEY,
    bigint_pk_col BIGINT DEFAULT NULL
);

CREATE TABLE bit_pk_table (
    id BIT PRIMARY KEY,
    bit_pk_col BIT DEFAULT NULL
);

CREATE TABLE date_pk_table (
    id DATE PRIMARY KEY,
    date_pk_col DATE DEFAULT NULL
);

CREATE TABLE time_pk_table (
    id TIME PRIMARY KEY,
    time_pk_col TIME DEFAULT NULL
);

CREATE TABLE datetime2_pk_table (
    id DATETIME2 PRIMARY KEY,
    datetime2_pk_col DATETIME2 DEFAULT NULL
);

CREATE TABLE datetimeoffset_pk_table (
    id DATETIMEOFFSET PRIMARY KEY,
    datetimeoffset_pk_col DATETIMEOFFSET DEFAULT NULL
);

CREATE TABLE datetime_pk_table (
    id DATETIME PRIMARY KEY,
    datetime_pk_col DATETIME DEFAULT NULL
);

CREATE TABLE smalldatetime_pk_table (
    id SMALLDATETIME PRIMARY KEY,
    smalldatetime_pk_col SMALLDATETIME DEFAULT NULL
);

CREATE TABLE char_pk_table (
    id CHAR(100) PRIMARY KEY,
    char_pk_col CHAR(100) DEFAULT NULL
);

CREATE TABLE varchar_pk_table (
    id VARCHAR(100) PRIMARY KEY,
    varchar_pk_col VARCHAR(100) DEFAULT NULL
);

CREATE TABLE nchar_pk_table (
    id NCHAR(100) PRIMARY KEY,
    nchar_pk_col NCHAR(100) DEFAULT NULL
);

CREATE TABLE nvarchar_pk_table (
    id NVARCHAR(100) PRIMARY KEY,
    nvarchar_pk_col NVARCHAR(100) DEFAULT NULL
);

CREATE TABLE binary_pk_table (
    id BINARY(100) PRIMARY KEY,
    binary_pk_col BINARY(100) DEFAULT NULL
);

CREATE TABLE varbinary_pk_table (
    id VARBINARY(100) PRIMARY KEY,
    varbinary_pk_col VARBINARY(100) DEFAULT NULL
);

CREATE TABLE uniqueidentifier_pk_table (
    id UNIQUEIDENTIFIER PRIMARY KEY,
    uniqueidentifier_pk_col UNIQUEIDENTIFIER DEFAULT NULL
);

CREATE TABLE geography_table (
    id INT PRIMARY KEY,
    geography_col GEOGRAPHY DEFAULT NULL
);

CREATE TABLE geometry_table (
    id INT PRIMARY KEY,
    geometry_col GEOMETRY DEFAULT NULL
);

CREATE TABLE hierarchyid_table (
    id INT PRIMARY KEY,
    hierarchyid_col HIERARCHYID DEFAULT NULL
);

CREATE TABLE sql_variant_table (
    id INT PRIMARY KEY,
    sql_variant_col SQL_VARIANT DEFAULT NULL
);

CREATE TABLE generated_pk_column (
    first_name_col VARCHAR(255) NOT NULL,
    last_name_col VARCHAR(255) NOT NULL,
    generated_column_col AS (CONCAT(first_name_col, ' ', last_name_col)) PERSISTED,
    PRIMARY KEY (generated_column_col)
);

CREATE TABLE generated_non_pk_column (
    id INT PRIMARY KEY,
    first_name_col VARCHAR(255) NOT NULL,
    last_name_col VARCHAR(255) NOT NULL,
    generated_column_col AS (CONCAT(first_name_col, ' ', last_name_col))
);

CREATE TABLE non_generated_to_generated_column (
    first_name_col VARCHAR(255) NOT NULL,
    last_name_col VARCHAR(255) NOT NULL,
    generated_column_col AS (CONCAT(first_name_col, ' ', last_name_col)),
    generated_column_pk_col VARCHAR(255) NOT NULL,
    PRIMARY KEY (generated_column_pk_col)
);

CREATE TABLE generated_to_non_generated_column (
    first_name_col VARCHAR(255) NOT NULL,
    last_name_col VARCHAR(255) NOT NULL,
    generated_column_col VARCHAR(255) NOT NULL,
    generated_column_pk_col AS (CONCAT(first_name_col, ' ', last_name_col)) PERSISTED,
    PRIMARY KEY (generated_column_pk_col)
);

INSERT INTO tinyint_table VALUES (1, 10), (2, 255), (3, 0), (4, NULL);
INSERT INTO tinyint_to_string_table VALUES (1, 10), (2, 255), (3, 0), (4, NULL);
INSERT INTO tinyint_pk_table VALUES (10, 10), (255, 255), (0, 0);

INSERT INTO smallint_table VALUES (1, 15), (2, 32767), (3, -32768), (4, NULL);
INSERT INTO smallint_to_string_table VALUES (1, 15), (2, 32767), (3, -32768), (4, NULL);
INSERT INTO smallint_pk_table VALUES (15, 15), (32767, 32767), (-32768, -32768);

INSERT INTO int_table VALUES (1, 30), (2, 2147483647), (3, -2147483648), (4, NULL);
INSERT INTO int_to_string_table VALUES (1, 30), (2, 2147483647), (3, -2147483648), (4, NULL);
INSERT INTO int_pk_table VALUES (30, 30), (2147483647, 2147483647), (-2147483648, -2147483648);

INSERT INTO bigint_table VALUES (1, 40), (2, 9223372036854775807), (3, -9223372036854775808), (4, NULL);
INSERT INTO bigint_to_string_table VALUES (1, 40), (2, 9223372036854775807), (3, -9223372036854775808), (4, NULL);
INSERT INTO bigint_pk_table VALUES (40, 40), (9223372036854775807, 9223372036854775807), (-9223372036854775808, -9223372036854775808);

INSERT INTO bit_table VALUES (1, 0), (2, 1), (3, NULL);
INSERT INTO bit_to_int64_table VALUES (1, 0), (2, 1), (3, NULL);
INSERT INTO bit_to_string_table VALUES (1, 0), (2, 1), (3, NULL);
INSERT INTO bit_pk_table VALUES (0, 0), (1, 1);

INSERT INTO decimal_table VALUES (1, 68.75), (2, 99999999999999999999999.999999999), (3, -99999999999999999999999.999999999), (4, NULL);
INSERT INTO decimal_to_string_table VALUES (1, 68.75), (2, 99999999999999999999999.999999999), (3, -99999999999999999999999.999999999), (4, NULL);
INSERT INTO decimal_to_float64_table VALUES (1, 68.75), (2, 99999999999999999999999.999999999), (3, -99999999999999999999999.999999999), (4, NULL);

INSERT INTO numeric_table VALUES (1, 68.75), (2, 99999999999999999999999.999999999), (3, -99999999999999999999999.999999999), (4, NULL);
INSERT INTO numeric_to_string_table VALUES (1, 68.75), (2, 99999999999999999999999.999999999), (3, -99999999999999999999999.999999999), (4, NULL);

INSERT INTO money_table VALUES (1, 922337203685477.5807), (2, -922337203685477.5808), (3, 100.50), (4, NULL);
INSERT INTO money_to_string_table VALUES (1, 922337203685477.5807), (2, -922337203685477.5808), (3, 100.50), (4, NULL);

INSERT INTO smallmoney_table VALUES (1, 214748.3647), (2, -214748.3648), (3, 50.25), (4, NULL);
INSERT INTO smallmoney_to_string_table VALUES (1, 214748.3647), (2, -214748.3648), (3, 50.25), (4, NULL);

INSERT INTO float_table VALUES (1, 45.56), (2, 1.79E+308), (3, -1.79E+308), (4, NULL);
INSERT INTO float_to_string_table VALUES (1, 45.56), (2, 1.79E+308), (3, -1.79E+308), (4, NULL);

INSERT INTO real_table VALUES (1, 45.56), (2, 3.40E+38), (3, -3.40E+38), (4, NULL);
INSERT INTO real_to_float64_table VALUES (1, 45.56), (2, 3.40E+38), (3, -3.40E+38), (4, NULL);
INSERT INTO real_to_string_table VALUES (1, 45.56), (2, 3.40E+38), (3, -3.40E+38), (4, NULL);

INSERT INTO date_table VALUES (1, '2022-09-17'), (2, '0001-01-01'), (3, '9999-12-31'), (4, NULL);
INSERT INTO date_to_string_table VALUES (1, '2022-09-17'), (2, '0001-01-01'), (3, '9999-12-31'), (4, NULL);
INSERT INTO date_pk_table VALUES ('2022-09-17', '2022-09-17'), ('0001-01-01', '0001-01-01'), ('9999-12-31', '9999-12-31');

INSERT INTO time_table VALUES (1, '15:50:00.0000000'), (2, '00:00:00.0000000'), (3, '23:59:59.9999999'), (4, NULL);
INSERT INTO time_pk_table VALUES ('15:50:00.0000000', '15:50:00.0000000'), ('00:00:00.0000000', '00:00:00.0000000'), ('23:59:59.9999999', '23:59:59.9999999');

INSERT INTO datetime2_table VALUES (1, '2022-08-05 08:23:11.1234567'), (2, '0001-01-01 00:00:00.0000000'), (3, '9999-12-31 23:59:59.9999999'), (4, NULL);
INSERT INTO datetime2_to_string_table VALUES (1, '2022-08-05 08:23:11.1234567'), (2, '0001-01-01 00:00:00.0000000'), (3, '9999-12-31 23:59:59.9999999'), (4, NULL);
INSERT INTO datetime2_pk_table VALUES ('2022-08-05 08:23:11.1234567', '2022-08-05 08:23:11.1234567'), ('0001-01-01 00:00:00.0000000', '0001-01-01 00:00:00.0000000'), ('9999-12-31 23:59:59.9999999', '9999-12-31 23:59:59.9999999');

INSERT INTO datetimeoffset_table VALUES (1, '2022-08-05 08:23:11.1234567 +00:00'), (2, '0001-01-01 00:00:00.0000000 +00:00'), (3, '9999-12-31 23:59:59.9999999 +14:00'), (4, NULL);
INSERT INTO datetimeoffset_to_string_table VALUES (1, '2022-08-05 08:23:11.1234567 +00:00'), (2, '0001-01-01 00:00:00.0000000 +00:00'), (3, '9999-12-31 23:59:59.9999999 +14:00'), (4, NULL);
INSERT INTO datetimeoffset_pk_table VALUES ('2022-08-05 08:23:11.1234567 +00:00', '2022-08-05 08:23:11.1234567 +00:00'), ('0001-01-01 00:00:00.0000000 +00:00', '0001-01-01 00:00:00.0000000 +00:00');

INSERT INTO datetime_table VALUES (1, '1998-01-23 12:45:56.000'), (2, '1753-01-01 00:00:00.000'), (3, '9999-12-31 23:59:59.997'), (4, NULL);
INSERT INTO datetime_to_string_table VALUES (1, '1998-01-23 12:45:56.000'), (2, '1753-01-01 00:00:00.000'), (3, '9999-12-31 23:59:59.997'), (4, NULL);
INSERT INTO datetime_pk_table VALUES ('1998-01-23 12:45:56.000', '1998-01-23 12:45:56.000'), ('1753-01-01 00:00:00.000', '1753-01-01 00:00:00.000'), ('9999-12-31 23:59:59.997', '9999-12-31 23:59:59.997');

INSERT INTO smalldatetime_table VALUES (1, '2022-08-05 08:23:00'), (2, '1900-01-01 00:00:00'), (3, '2079-06-06 23:59:00'), (4, NULL);
INSERT INTO smalldatetime_to_string_table VALUES (1, '2022-08-05 08:23:00'), (2, '1900-01-01 00:00:00'), (3, '2079-06-06 23:59:00'), (4, NULL);
INSERT INTO smalldatetime_pk_table VALUES ('2022-08-05 08:23:00', '2022-08-05 08:23:00'), ('1900-01-01 00:00:00', '1900-01-01 00:00:00'), ('2079-06-06 23:59:00', '2079-06-06 23:59:00');

INSERT INTO char_table VALUES (1, 'a'), (2, 'sample_char'), (3, NULL);
INSERT INTO char_to_bytes_table VALUES (1, 'a'), (2, 'sample_char'), (3, NULL);
INSERT INTO char_pk_table VALUES ('pk1', 'val1'), ('pk2', 'val2');

INSERT INTO varchar_table VALUES (1, 'abc'), (2, 'test_varchar'), (3, NULL);
INSERT INTO varchar_to_bytes_table VALUES (1, 'abc'), (2, 'test_varchar'), (3, NULL);
INSERT INTO varchar_pk_table VALUES ('vpk1', 'vval1'), ('vpk2', 'vval2');

INSERT INTO text_table VALUES (1, 'sample text data'), (2, 'extended text'), (3, NULL);

INSERT INTO nchar_table VALUES (1, N'a'), (2, N'sample_nchar'), (3, NULL);
INSERT INTO nchar_to_bytes_table VALUES (1, N'a'), (2, N'sample_nchar'), (3, NULL);
INSERT INTO nchar_pk_table VALUES (N'npk1', N'nval1'), (N'npk2', N'nval2');

INSERT INTO nvarchar_table VALUES (1, N'abc'), (2, N'unicode_ñ_ä_test'), (3, NULL);
INSERT INTO nvarchar_to_bytes_table VALUES (1, N'abc'), (2, N'unicode_ñ_ä_test'), (3, NULL);
INSERT INTO nvarchar_pk_table VALUES (N'nvpk1', N'nvval1'), (N'nvpk2', N'nvval2');

INSERT INTO ntext_table VALUES (1, N'sample ntext data'), (2, N'extended ntext'), (3, NULL);

INSERT INTO binary_table VALUES (1, 0x1234), (2, 0x00FF), (3, NULL);
INSERT INTO binary_to_string_table VALUES (1, 0x1234), (2, 0x00FF), (3, NULL);
INSERT INTO binary_pk_table VALUES (0x1234, 0x1234), (0x00FF, 0x00FF);

INSERT INTO varbinary_table VALUES (1, 0x1234ABCD), (2, 0xCAFEBABE), (3, NULL);
INSERT INTO varbinary_to_string_table VALUES (1, 0x1234ABCD), (2, 0xCAFEBABE), (3, NULL);
INSERT INTO varbinary_pk_table VALUES (0x1234ABCD, 0x1234ABCD), (0xCAFEBABE, 0xCAFEBABE);

INSERT INTO image_table VALUES (1, 0x89504E47), (2, 0xFFD8FFE0), (3, NULL);
INSERT INTO image_to_string_table VALUES (1, 0x89504E47), (2, 0xFFD8FFE0), (3, NULL);

INSERT INTO uniqueidentifier_table VALUES (1, '6F9619FF-8B86-D011-B42D-00C04FC964FF'), (2, 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'), (3, NULL);
INSERT INTO uniqueidentifier_pk_table VALUES ('6F9619FF-8B86-D011-B42D-00C04FC964FF', '6F9619FF-8B86-D011-B42D-00C04FC964FF'), ('A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11');

INSERT INTO xml_table VALUES (1, '<root><elem>test</elem></root>'), (2, '<user id="1"><name>sqlserver</name></user>'), (3, NULL);

INSERT INTO generated_pk_column VALUES ('AA', 'BB');
INSERT INTO generated_non_pk_column VALUES (1, 'AA', 'BB'), (10, 'AA', 'BB');
INSERT INTO non_generated_to_generated_column VALUES ('AA', 'BB', 'AA ');
INSERT INTO generated_to_non_generated_column VALUES ('AA', 'BB', 'AA ');
