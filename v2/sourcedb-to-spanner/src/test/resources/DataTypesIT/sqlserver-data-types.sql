CREATE TABLE [tinyint_table] (
    [id] INT PRIMARY KEY,
    [tinyint_col] TINYINT DEFAULT NULL
);
INSERT INTO [tinyint_table] ([id], [tinyint_col]) VALUES (1, 0), (2, 255), (3, 128), (4, 42), (5, NULL);

CREATE TABLE [tinyint_to_string_table] (
    [id] INT PRIMARY KEY,
    [tinyint_to_string_col] TINYINT DEFAULT NULL
);
INSERT INTO [tinyint_to_string_table] ([id], [tinyint_to_string_col]) VALUES (1, 0), (2, 255), (3, 128), (4, 42), (5, NULL);

CREATE TABLE [tinyint_pk_table] (
    [id] TINYINT PRIMARY KEY,
    [tinyint_pk_col] TINYINT NOT NULL
);
INSERT INTO [tinyint_pk_table] ([id], [tinyint_pk_col]) VALUES (0, 0), (255, 255), (128, 128), (42, 42);

CREATE TABLE [smallint_table] (
    [id] INT PRIMARY KEY,
    [smallint_col] SMALLINT DEFAULT NULL
);
INSERT INTO [smallint_table] ([id], [smallint_col]) VALUES (1, -32768), (2, 32767), (3, 0), (4, 15), (5, NULL);

CREATE TABLE [smallint_to_string_table] (
    [id] INT PRIMARY KEY,
    [smallint_to_string_col] SMALLINT DEFAULT NULL
);
INSERT INTO [smallint_to_string_table] ([id], [smallint_to_string_col]) VALUES (1, -32768), (2, 32767), (3, 0), (4, 15), (5, NULL);

CREATE TABLE [smallint_pk_table] (
    [id] SMALLINT PRIMARY KEY,
    [smallint_pk_col] SMALLINT NOT NULL
);
INSERT INTO [smallint_pk_table] ([id], [smallint_pk_col]) VALUES (-32768, -32768), (32767, 32767), (0, 0), (15, 15);

CREATE TABLE [int_table] (
    [id] INT PRIMARY KEY,
    [int_col] INT DEFAULT NULL
);
INSERT INTO [int_table] ([id], [int_col]) VALUES (1, -2147483648), (2, 2147483647), (3, 0), (4, 30), (5, NULL);

CREATE TABLE [int_to_string_table] (
    [id] INT PRIMARY KEY,
    [int_to_string_col] INT DEFAULT NULL
);
INSERT INTO [int_to_string_table] ([id], [int_to_string_col]) VALUES (1, -2147483648), (2, 2147483647), (3, 0), (4, 30), (5, NULL);

CREATE TABLE [int_pk_table] (
    [id] INT PRIMARY KEY,
    [int_pk_col] INT NOT NULL
);
INSERT INTO [int_pk_table] ([id], [int_pk_col]) VALUES (-2147483648, -2147483648), (2147483647, 2147483647), (0, 0), (30, 30);

CREATE TABLE [bigint_table] (
    [id] INT PRIMARY KEY,
    [bigint_col] BIGINT DEFAULT NULL
);
INSERT INTO [bigint_table] ([id], [bigint_col]) VALUES (1, -9223372036854775808), (2, 9223372036854775807), (3, 0), (4, 40), (5, NULL);

CREATE TABLE [bigint_to_string_table] (
    [id] INT PRIMARY KEY,
    [bigint_to_string_col] BIGINT DEFAULT NULL
);
INSERT INTO [bigint_to_string_table] ([id], [bigint_to_string_col]) VALUES (1, -9223372036854775808), (2, 9223372036854775807), (3, 0), (4, 40), (5, NULL);

CREATE TABLE [bigint_pk_table] (
    [id] BIGINT PRIMARY KEY,
    [bigint_pk_col] BIGINT NOT NULL
);
INSERT INTO [bigint_pk_table] ([id], [bigint_pk_col]) VALUES (-9223372036854775808, -9223372036854775808), (9223372036854775807, 9223372036854775807), (0, 0), (40, 40);

CREATE TABLE [bit_table] (
    [id] INT PRIMARY KEY,
    [bit_col] BIT DEFAULT NULL
);
INSERT INTO [bit_table] ([id], [bit_col]) VALUES (1, 0), (2, 1), (3, NULL);

CREATE TABLE [bit_to_int64_table] (
    [id] INT PRIMARY KEY,
    [bit_to_int64_col] BIT DEFAULT NULL
);
INSERT INTO [bit_to_int64_table] ([id], [bit_to_int64_col]) VALUES (1, 0), (2, 1), (3, NULL);

CREATE TABLE [bit_to_string_table] (
    [id] INT PRIMARY KEY,
    [bit_to_string_col] BIT DEFAULT NULL
);
INSERT INTO [bit_to_string_table] ([id], [bit_to_string_col]) VALUES (1, 0), (2, 1), (3, NULL);

CREATE TABLE [bit_pk_table] (
    [id] BIT PRIMARY KEY,
    [bit_pk_col] BIT NOT NULL
);
INSERT INTO [bit_pk_table] ([id], [bit_pk_col]) VALUES (0, 0), (1, 1);

CREATE TABLE [decimal_table] (
    [id] INT PRIMARY KEY,
    [decimal_col] DECIMAL(28, 9) DEFAULT NULL
);
INSERT INTO [decimal_table] ([id], [decimal_col]) VALUES (1, 68.75), (2, 9999999999999999999.999999999), (3, -9999999999999999999.999999999), (4, 0.0), (5, NULL);

CREATE TABLE [decimal_to_float64_table] (
    [id] INT PRIMARY KEY,
    [decimal_to_float64_col] DECIMAL(28, 9) DEFAULT NULL
);
INSERT INTO [decimal_to_float64_table] ([id], [decimal_to_float64_col]) VALUES (1, 68.75), (2, 9999999999999999999.999999999), (3, -9999999999999999999.999999999), (4, 0.0), (5, NULL);

CREATE TABLE [decimal_to_string_table] (
    [id] INT PRIMARY KEY,
    [decimal_to_string_col] DECIMAL(28, 9) DEFAULT NULL
);
INSERT INTO [decimal_to_string_table] ([id], [decimal_to_string_col]) VALUES (1, 68.75), (2, 9999999999999999999.999999999), (3, -9999999999999999999.999999999), (4, 0.0), (5, NULL);

CREATE TABLE [numeric_table] (
    [id] INT PRIMARY KEY,
    [numeric_col] NUMERIC(28, 9) DEFAULT NULL
);
INSERT INTO [numeric_table] ([id], [numeric_col]) VALUES (1, 68.75), (2, 9999999999999999999.999999999), (3, -9999999999999999999.999999999), (4, 0.0), (5, NULL);

CREATE TABLE [numeric_to_float64_table] (
    [id] INT PRIMARY KEY,
    [numeric_to_float64_col] NUMERIC(28, 9) DEFAULT NULL
);
INSERT INTO [numeric_to_float64_table] ([id], [numeric_to_float64_col]) VALUES (1, 68.75), (2, 9999999999999999999.999999999), (3, -9999999999999999999.999999999), (4, 0.0), (5, NULL);

CREATE TABLE [numeric_to_string_table] (
    [id] INT PRIMARY KEY,
    [numeric_to_string_col] NUMERIC(28, 9) DEFAULT NULL
);
INSERT INTO [numeric_to_string_table] ([id], [numeric_to_string_col]) VALUES (1, 68.75), (2, 9999999999999999999.999999999), (3, -9999999999999999999.999999999), (4, 0.0), (5, NULL);

CREATE TABLE [numeric_pk_table] (
    [id] NUMERIC(28, 9) PRIMARY KEY,
    [numeric_pk_col] NUMERIC(28, 9) NOT NULL
);
INSERT INTO [numeric_pk_table] ([id], [numeric_pk_col]) VALUES (68.75, 68.75), (9999999999999999999.999999999, 9999999999999999999.999999999), (-9999999999999999999.999999999, -9999999999999999999.999999999), (0.0, 0.0);

CREATE TABLE [money_table] (
    [id] INT PRIMARY KEY,
    [money_col] MONEY DEFAULT NULL
);
INSERT INTO [money_table] ([id], [money_col]) VALUES (1, -922337203685477.5808), (2, 922337203685477.5807), (3, 123.45), (4, 0.00), (5, NULL);

CREATE TABLE [money_to_float64_table] (
    [id] INT PRIMARY KEY,
    [money_to_float64_col] MONEY DEFAULT NULL
);
INSERT INTO [money_to_float64_table] ([id], [money_to_float64_col]) VALUES (1, -922337203685477.5808), (2, 922337203685477.5807), (3, 123.45), (4, 0.00), (5, NULL);

CREATE TABLE [money_to_string_table] (
    [id] INT PRIMARY KEY,
    [money_to_string_col] MONEY DEFAULT NULL
);
INSERT INTO [money_to_string_table] ([id], [money_to_string_col]) VALUES (1, -922337203685477.5808), (2, 922337203685477.5807), (3, 123.45), (4, 0.00), (5, NULL);

CREATE TABLE [smallmoney_table] (
    [id] INT PRIMARY KEY,
    [smallmoney_col] SMALLMONEY DEFAULT NULL
);
INSERT INTO [smallmoney_table] ([id], [smallmoney_col]) VALUES (1, -214748.3648), (2, 214748.3647), (3, 50.25), (4, 0.00), (5, NULL);

CREATE TABLE [smallmoney_to_float64_table] (
    [id] INT PRIMARY KEY,
    [smallmoney_to_float64_col] SMALLMONEY DEFAULT NULL
);
INSERT INTO [smallmoney_to_float64_table] ([id], [smallmoney_to_float64_col]) VALUES (1, -214748.3648), (2, 214748.3647), (3, 50.25), (4, 0.00), (5, NULL);

CREATE TABLE [smallmoney_to_string_table] (
    [id] INT PRIMARY KEY,
    [smallmoney_to_string_col] SMALLMONEY DEFAULT NULL
);
INSERT INTO [smallmoney_to_string_table] ([id], [smallmoney_to_string_col]) VALUES (1, -214748.3648), (2, 214748.3647), (3, 50.25), (4, 0.00), (5, NULL);

CREATE TABLE [float_table] (
    [id] INT PRIMARY KEY,
    [float_col] FLOAT DEFAULT NULL
);
INSERT INTO [float_table] ([id], [float_col]) VALUES (1, -1.79E+308), (2, 1.79E+308), (3, 45.56), (4, 0.0), (5, NULL);

CREATE TABLE [float_to_string_table] (
    [id] INT PRIMARY KEY,
    [float_to_string_col] FLOAT DEFAULT NULL
);
INSERT INTO [float_to_string_table] ([id], [float_to_string_col]) VALUES (1, -1.79E+308), (2, 1.79E+308), (3, 45.56), (4, 0.0), (5, NULL);

CREATE TABLE [real_table] (
    [id] INT PRIMARY KEY,
    [real_col] REAL DEFAULT NULL
);
INSERT INTO [real_table] ([id], [real_col]) VALUES (1, -3.40E+38), (2, 3.40E+38), (3, 12.34), (4, 0.0), (5, NULL);

CREATE TABLE [real_to_float64_table] (
    [id] INT PRIMARY KEY,
    [real_to_float64_col] REAL DEFAULT NULL
);
INSERT INTO [real_to_float64_table] ([id], [real_to_float64_col]) VALUES (1, -3.40E+38), (2, 3.40E+38), (3, 12.34), (4, 0.0), (5, NULL);

CREATE TABLE [real_to_string_table] (
    [id] INT PRIMARY KEY,
    [real_to_string_col] REAL DEFAULT NULL
);
INSERT INTO [real_to_string_table] ([id], [real_to_string_col]) VALUES (1, -3.40E+38), (2, 3.40E+38), (3, 12.34), (4, 0.0), (5, NULL);

CREATE TABLE [date_table] (
    [id] INT PRIMARY KEY,
    [date_col] DATE DEFAULT NULL
);
INSERT INTO [date_table] ([id], [date_col]) VALUES (1, '0001-01-01'), (2, '9999-12-31'), (3, '2022-09-17'), (4, NULL);

CREATE TABLE [date_to_string_table] (
    [id] INT PRIMARY KEY,
    [date_to_string_col] DATE DEFAULT NULL
);
INSERT INTO [date_to_string_table] ([id], [date_to_string_col]) VALUES (1, '0001-01-01'), (2, '9999-12-31'), (3, '2022-09-17'), (4, NULL);

CREATE TABLE [date_pk_table] (
    [id] DATE PRIMARY KEY,
    [date_pk_col] DATE NOT NULL
);
INSERT INTO [date_pk_table] ([id], [date_pk_col]) VALUES ('0001-01-01', '0001-01-01'), ('9999-12-31', '9999-12-31'), ('2022-09-17', '2022-09-17');

CREATE TABLE [time_table] (
    [id] INT PRIMARY KEY,
    [time_col] TIME DEFAULT NULL
);
INSERT INTO [time_table] ([id], [time_col]) VALUES (1, '00:00:00.0000000'), (2, '23:59:59.9999999'), (3, '15:30:45.1234567'), (4, NULL);

CREATE TABLE [time_to_bytes_table] (
    [id] INT PRIMARY KEY,
    [time_to_bytes_col] TIME DEFAULT NULL
);
INSERT INTO [time_to_bytes_table] ([id], [time_to_bytes_col]) VALUES (1, '00:00:00.0000000'), (2, '23:59:59.9999999'), (3, '15:30:45.1234567'), (4, NULL);

CREATE TABLE [time_pk_table] (
    [id] TIME PRIMARY KEY,
    [time_pk_col] TIME NOT NULL
);
INSERT INTO [time_pk_table] ([id], [time_pk_col]) VALUES ('00:00:00.0000000', '00:00:00.0000000'), ('23:59:59.9999999', '23:59:59.9999999'), ('15:30:45.1234567', '15:30:45.1234567');

CREATE TABLE [datetime2_table] (
    [id] INT PRIMARY KEY,
    [datetime2_col] DATETIME2 DEFAULT NULL
);
INSERT INTO [datetime2_table] ([id], [datetime2_col]) VALUES (1, '1970-01-01 00:00:00'), (2, '2023-05-15 12:30:00'), (3, '9999-12-31 23:59:59'), (4, NULL);

CREATE TABLE [datetime2_to_string_table] (
    [id] INT PRIMARY KEY,
    [datetime2_to_string_col] DATETIME2 DEFAULT NULL
);
INSERT INTO [datetime2_to_string_table] ([id], [datetime2_to_string_col]) VALUES (1, '1970-01-01 00:00:00'), (2, '2023-05-15 12:30:00'), (3, '9999-12-31 23:59:59'), (4, NULL);

CREATE TABLE [datetime2_pk_table] (
    [id] DATETIME2 PRIMARY KEY,
    [datetime2_pk_col] DATETIME2 NOT NULL
);
INSERT INTO [datetime2_pk_table] ([id], [datetime2_pk_col]) VALUES ('1970-01-01 00:00:00', '1970-01-01 00:00:00'), ('2023-05-15 12:30:00', '2023-05-15 12:30:00'), ('9999-12-31 23:59:59', '9999-12-31 23:59:59');

CREATE TABLE [datetimeoffset_table] (
    [id] INT PRIMARY KEY,
    [datetimeoffset_col] DATETIMEOFFSET DEFAULT NULL
);
INSERT INTO [datetimeoffset_table] ([id], [datetimeoffset_col]) VALUES (1, '1970-01-01 00:00:00 +00:00'), (2, '2023-05-15 12:30:00 +00:00'), (3, '9999-12-31 23:59:59 +00:00'), (4, NULL);

CREATE TABLE [datetimeoffset_to_string_table] (
    [id] INT PRIMARY KEY,
    [datetimeoffset_to_string_col] DATETIMEOFFSET DEFAULT NULL
);
INSERT INTO [datetimeoffset_to_string_table] ([id], [datetimeoffset_to_string_col]) VALUES (1, '1970-01-01 00:00:00 +00:00'), (2, '2023-05-15 12:30:00 +00:00'), (3, '9999-12-31 23:59:59 +00:00'), (4, NULL);

CREATE TABLE [datetimeoffset_pk_table] (
    [id] DATETIMEOFFSET PRIMARY KEY,
    [datetimeoffset_pk_col] DATETIMEOFFSET NOT NULL
);
INSERT INTO [datetimeoffset_pk_table] ([id], [datetimeoffset_pk_col]) VALUES ('1970-01-01 00:00:00 +00:00', '1970-01-01 00:00:00 +00:00'), ('2023-05-15 12:30:00 +00:00', '2023-05-15 12:30:00 +00:00'), ('9999-12-31 23:59:59 +00:00', '9999-12-31 23:59:59 +00:00');

CREATE TABLE [datetime_table] (
    [id] INT PRIMARY KEY,
    [datetime_col] DATETIME DEFAULT NULL
);
INSERT INTO [datetime_table] ([id], [datetime_col]) VALUES (1, '1970-01-01 00:00:00'), (2, '1998-01-23 12:45:56'), (3, '9999-12-31 23:59:59'), (4, NULL);

CREATE TABLE [datetime_to_string_table] (
    [id] INT PRIMARY KEY,
    [datetime_to_string_col] DATETIME DEFAULT NULL
);
INSERT INTO [datetime_to_string_table] ([id], [datetime_to_string_col]) VALUES (1, '1970-01-01 00:00:00'), (2, '1998-01-23 12:45:56'), (3, '9999-12-31 23:59:59'), (4, NULL);

CREATE TABLE [datetime_pk_table] (
    [id] DATETIME PRIMARY KEY,
    [datetime_pk_col] DATETIME NOT NULL
);
INSERT INTO [datetime_pk_table] ([id], [datetime_pk_col]) VALUES ('1970-01-01 00:00:00', '1970-01-01 00:00:00'), ('1998-01-23 12:45:56', '1998-01-23 12:45:56'), ('9999-12-31 23:59:59', '9999-12-31 23:59:59');

CREATE TABLE [smalldatetime_table] (
    [id] INT PRIMARY KEY,
    [smalldatetime_col] SMALLDATETIME DEFAULT NULL
);
INSERT INTO [smalldatetime_table] ([id], [smalldatetime_col]) VALUES (1, '1900-01-01 00:00:00'), (2, '2023-05-15 12:30:00'), (3, '2079-06-06 23:59:00'), (4, NULL);

CREATE TABLE [smalldatetime_to_string_table] (
    [id] INT PRIMARY KEY,
    [smalldatetime_to_string_col] SMALLDATETIME DEFAULT NULL
);
INSERT INTO [smalldatetime_to_string_table] ([id], [smalldatetime_to_string_col]) VALUES (1, '1900-01-01 00:00:00'), (2, '2023-05-15 12:30:00'), (3, '2079-06-06 23:59:00'), (4, NULL);

CREATE TABLE [smalldatetime_pk_table] (
    [id] SMALLDATETIME PRIMARY KEY,
    [smalldatetime_pk_col] SMALLDATETIME NOT NULL
);
INSERT INTO [smalldatetime_pk_table] ([id], [smalldatetime_pk_col]) VALUES ('1900-01-01 00:00:00', '1900-01-01 00:00:00'), ('2023-05-15 12:30:00', '2023-05-15 12:30:00'), ('2079-06-06 23:59:00', '2079-06-06 23:59:00');

CREATE TABLE [char_table] (
    [id] INT PRIMARY KEY,
    [char_col] CHAR(10) DEFAULT NULL
);
INSERT INTO [char_table] ([id], [char_col]) VALUES (1, 'a'), (2, 'hello'), (3, NULL);

CREATE TABLE [char_to_bytes_table] (
    [id] INT PRIMARY KEY,
    [char_to_bytes_col] CHAR(10) DEFAULT NULL
);
INSERT INTO [char_to_bytes_table] ([id], [char_to_bytes_col]) VALUES (1, 'a'), (2, 'hello'), (3, NULL);

CREATE TABLE [char_pk_table] (
    [id] CHAR(10) PRIMARY KEY,
    [char_pk_col] CHAR(10) NOT NULL
);
INSERT INTO [char_pk_table] ([id], [char_pk_col]) VALUES ('a', 'a'), ('hello', 'hello');

CREATE TABLE [varchar_table] (
    [id] INT PRIMARY KEY,
    [varchar_col] VARCHAR(255) DEFAULT NULL
);
INSERT INTO [varchar_table] ([id], [varchar_col]) VALUES (1, 'a'), (2, 'test varchar'), (3, NULL);

CREATE TABLE [varchar_to_bytes_table] (
    [id] INT PRIMARY KEY,
    [varchar_to_bytes_col] VARCHAR(255) DEFAULT NULL
);
INSERT INTO [varchar_to_bytes_table] ([id], [varchar_to_bytes_col]) VALUES (1, 'a'), (2, 'test varchar'), (3, NULL);

CREATE TABLE [varchar_pk_table] (
    [id] VARCHAR(255) PRIMARY KEY,
    [varchar_pk_col] VARCHAR(255) NOT NULL
);
INSERT INTO [varchar_pk_table] ([id], [varchar_pk_col]) VALUES ('a', 'a'), ('test varchar', 'test varchar');

CREATE TABLE [text_table] (
    [id] INT PRIMARY KEY,
    [text_col] TEXT DEFAULT NULL
);
INSERT INTO [text_table] ([id], [text_col]) VALUES (1, 'a'), (2, 'long text content'), (3, NULL);

CREATE TABLE [text_to_bytes_table] (
    [id] INT PRIMARY KEY,
    [text_to_bytes_col] TEXT DEFAULT NULL
);
INSERT INTO [text_to_bytes_table] ([id], [text_to_bytes_col]) VALUES (1, 'a'), (2, 'long text content'), (3, NULL);

CREATE TABLE [nchar_table] (
    [id] INT PRIMARY KEY,
    [nchar_col] NCHAR(10) DEFAULT NULL
);
INSERT INTO [nchar_table] ([id], [nchar_col]) VALUES (1, N'a'), (2, N'unicode'), (3, NULL);

CREATE TABLE [nchar_to_bytes_table] (
    [id] INT PRIMARY KEY,
    [nchar_to_bytes_col] NCHAR(10) DEFAULT NULL
);
INSERT INTO [nchar_to_bytes_table] ([id], [nchar_to_bytes_col]) VALUES (1, N'a'), (2, N'unicode'), (3, NULL);

CREATE TABLE [nchar_pk_table] (
    [id] NCHAR(10) PRIMARY KEY,
    [nchar_pk_col] NCHAR(10) NOT NULL
);
INSERT INTO [nchar_pk_table] ([id], [nchar_pk_col]) VALUES (N'a', N'a'), (N'unicode', N'unicode');

CREATE TABLE [nvarchar_table] (
    [id] INT PRIMARY KEY,
    [nvarchar_col] NVARCHAR(255) DEFAULT NULL
);
INSERT INTO [nvarchar_table] ([id], [nvarchar_col]) VALUES (1, N'a'), (2, N'nvarchar test'), (3, NULL);

CREATE TABLE [nvarchar_to_bytes_table] (
    [id] INT PRIMARY KEY,
    [nvarchar_to_bytes_col] NVARCHAR(255) DEFAULT NULL
);
INSERT INTO [nvarchar_to_bytes_table] ([id], [nvarchar_to_bytes_col]) VALUES (1, N'a'), (2, N'nvarchar test'), (3, NULL);

CREATE TABLE [nvarchar_pk_table] (
    [id] NVARCHAR(255) PRIMARY KEY,
    [nvarchar_pk_col] NVARCHAR(255) NOT NULL
);
INSERT INTO [nvarchar_pk_table] ([id], [nvarchar_pk_col]) VALUES (N'a', N'a'), (N'nvarchar test', N'nvarchar test');

CREATE TABLE [ntext_table] (
    [id] INT PRIMARY KEY,
    [ntext_col] NTEXT DEFAULT NULL
);
INSERT INTO [ntext_table] ([id], [ntext_col]) VALUES (1, N'a'), (2, N'ntext content'), (3, NULL);

CREATE TABLE [ntext_to_bytes_table] (
    [id] INT PRIMARY KEY,
    [ntext_to_bytes_col] NTEXT DEFAULT NULL
);
INSERT INTO [ntext_to_bytes_table] ([id], [ntext_to_bytes_col]) VALUES (1, N'a'), (2, N'ntext content'), (3, NULL);

CREATE TABLE [binary_table] (
    [id] INT PRIMARY KEY,
    [binary_col] BINARY(4) DEFAULT NULL
);
INSERT INTO [binary_table] ([id], [binary_col]) VALUES (1, 0x00000000), (2, 0x12345678), (3, NULL);

CREATE TABLE [binary_to_string_table] (
    [id] INT PRIMARY KEY,
    [binary_to_string_col] BINARY(4) DEFAULT NULL
);
INSERT INTO [binary_to_string_table] ([id], [binary_to_string_col]) VALUES (1, 0x00000000), (2, 0x12345678), (3, NULL);

CREATE TABLE [binary_pk_table] (
    [id] BINARY(4) PRIMARY KEY,
    [binary_pk_col] BINARY(4) NOT NULL
);
INSERT INTO [binary_pk_table] ([id], [binary_pk_col]) VALUES (0x00000000, 0x00000000), (0x12345678, 0x12345678);

CREATE TABLE [varbinary_table] (
    [id] INT PRIMARY KEY,
    [varbinary_col] VARBINARY(255) DEFAULT NULL
);
INSERT INTO [varbinary_table] ([id], [varbinary_col]) VALUES (1, 0x00), (2, 0xABCDEF), (3, NULL);

CREATE TABLE [varbinary_to_string_table] (
    [id] INT PRIMARY KEY,
    [varbinary_to_string_col] VARBINARY(255) DEFAULT NULL
);
INSERT INTO [varbinary_to_string_table] ([id], [varbinary_to_string_col]) VALUES (1, 0x00), (2, 0xABCDEF), (3, NULL);

CREATE TABLE [varbinary_pk_table] (
    [id] VARBINARY(255) PRIMARY KEY,
    [varbinary_pk_col] VARBINARY(255) NOT NULL
);
INSERT INTO [varbinary_pk_table] ([id], [varbinary_pk_col]) VALUES (0x01, 0x01), (0xABCDEF, 0xABCDEF);

CREATE TABLE [image_table] (
    [id] INT PRIMARY KEY,
    [image_col] IMAGE DEFAULT NULL
);
INSERT INTO [image_table] ([id], [image_col]) VALUES (1, 0x00), (2, 0x01020304), (3, NULL);

CREATE TABLE [image_to_string_table] (
    [id] INT PRIMARY KEY,
    [image_to_string_col] IMAGE DEFAULT NULL
);
INSERT INTO [image_to_string_table] ([id], [image_to_string_col]) VALUES (1, 0x00), (2, 0x01020304), (3, NULL);

CREATE TABLE [uniqueidentifier_table] (
    [id] INT PRIMARY KEY,
    [uniqueidentifier_col] UNIQUEIDENTIFIER DEFAULT NULL
);
INSERT INTO [uniqueidentifier_table] ([id], [uniqueidentifier_col]) VALUES (1, '6F9619FF-8B86-D011-B42D-00C04FC964FF'), (2, 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'), (3, NULL);

CREATE TABLE [uniqueidentifier_to_bytes_table] (
    [id] INT PRIMARY KEY,
    [uniqueidentifier_to_bytes_col] UNIQUEIDENTIFIER DEFAULT NULL
);
INSERT INTO [uniqueidentifier_to_bytes_table] ([id], [uniqueidentifier_to_bytes_col]) VALUES (1, '6F9619FF-8B86-D011-B42D-00C04FC964FF'), (2, 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'), (3, NULL);

CREATE TABLE [uniqueidentifier_to_string_table] (
    [id] INT PRIMARY KEY,
    [uniqueidentifier_to_string_col] UNIQUEIDENTIFIER DEFAULT NULL
);
INSERT INTO [uniqueidentifier_to_string_table] ([id], [uniqueidentifier_to_string_col]) VALUES (1, '6F9619FF-8B86-D011-B42D-00C04FC964FF'), (2, 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'), (3, NULL);

CREATE TABLE [uniqueidentifier_pk_table] (
    [id] UNIQUEIDENTIFIER PRIMARY KEY,
    [uniqueidentifier_pk_col] UNIQUEIDENTIFIER NOT NULL
);
INSERT INTO [uniqueidentifier_pk_table] ([id], [uniqueidentifier_pk_col]) VALUES ('6F9619FF-8B86-D011-B42D-00C04FC964FF', '6F9619FF-8B86-D011-B42D-00C04FC964FF'), ('A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11');

CREATE TABLE [xml_table] (
    [id] INT PRIMARY KEY,
    [xml_col] XML DEFAULT NULL
);
INSERT INTO [xml_table] ([id], [xml_col]) VALUES (1, '<root><child>value</child></root>'), (2, '<item id="1"/>'), (3, NULL);

CREATE TABLE [xml_to_bytes_table] (
    [id] INT PRIMARY KEY,
    [xml_to_bytes_col] XML DEFAULT NULL
);
INSERT INTO [xml_to_bytes_table] ([id], [xml_to_bytes_col]) VALUES (1, '<root><child>value</child></root>'), (2, '<item id="1"/>'), (3, NULL);

CREATE TABLE [rowversion_table] (
    [id] INT PRIMARY KEY,
    [rowversion_col] ROWVERSION
);
INSERT INTO [rowversion_table] ([id]) VALUES (1), (2);

CREATE TABLE [rowversion_to_bytes_table] (
    [id] INT PRIMARY KEY,
    [rowversion_to_bytes_col] ROWVERSION
);
INSERT INTO [rowversion_to_bytes_table] ([id]) VALUES (1), (2);

CREATE TABLE [rowversion_to_int64_table] (
    [id] INT PRIMARY KEY,
    [rowversion_to_int64_col] ROWVERSION
);
INSERT INTO [rowversion_to_int64_table] ([id]) VALUES (1), (2);

CREATE TABLE [timestamp_table] (
    [id] INT PRIMARY KEY,
    [timestamp_col] TIMESTAMP
);
INSERT INTO [timestamp_table] ([id]) VALUES (1), (2);

CREATE TABLE [timestamp_to_bytes_table] (
    [id] INT PRIMARY KEY,
    [timestamp_to_bytes_col] TIMESTAMP
);
INSERT INTO [timestamp_to_bytes_table] ([id]) VALUES (1), (2);

CREATE TABLE [timestamp_to_int64_table] (
    [id] INT PRIMARY KEY,
    [timestamp_to_int64_col] TIMESTAMP
);
INSERT INTO [timestamp_to_int64_table] ([id]) VALUES (1), (2);

CREATE TABLE [json_table] (
    [id] INT PRIMARY KEY,
    [json_col] JSON DEFAULT NULL
);
INSERT INTO [json_table] ([id], [json_col]) VALUES (1, '{"key": "val1"}'), (2, NULL);

CREATE TABLE [json_to_string_table] (
    [id] INT PRIMARY KEY,
    [json_to_string_col] JSON DEFAULT NULL
);
INSERT INTO [json_to_string_table] ([id], [json_to_string_col]) VALUES (1, '{"key": "val1"}'), (2, NULL);

CREATE TABLE [vector_table] (
    [id] INT PRIMARY KEY,
    [vector_col] VECTOR(3) NULL
);
INSERT INTO [vector_table] ([id], [vector_col]) VALUES (1, '[1.5, 2.5, 3.5]'), (2, NULL);



