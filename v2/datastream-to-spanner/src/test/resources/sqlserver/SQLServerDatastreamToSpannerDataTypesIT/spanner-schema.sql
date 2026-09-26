CREATE TABLE `varchar_table` (
    `id` INT64,
    `varchar_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `tinyint_table` (
    `id` INT64,
    `tinyint_col` INT64
) PRIMARY KEY (`id`);

CREATE TABLE `smallint_table` (
    `id` INT64,
    `smallint_col` INT64
) PRIMARY KEY (`id`);

CREATE TABLE `int_table` (
    `id` INT64,
    `int_col` INT64
) PRIMARY KEY (`id`);

CREATE TABLE `bigint_table` (
    `id` INT64,
    `bigint_col` INT64
) PRIMARY KEY (`id`);

CREATE TABLE `bit_table` (
    `id` INT64,
    `bit_col` BOOL
) PRIMARY KEY (`id`);

CREATE TABLE `decimal_table` (
    `id` INT64,
    `decimal_col` NUMERIC
) PRIMARY KEY (`id`);

CREATE TABLE `numeric_table` (
    `id` INT64,
    `numeric_col` NUMERIC
) PRIMARY KEY (`id`);

CREATE TABLE `money_table` (
    `id` INT64,
    `money_col` NUMERIC
) PRIMARY KEY (`id`);

CREATE TABLE `smallmoney_table` (
    `id` INT64,
    `smallmoney_col` NUMERIC
) PRIMARY KEY (`id`);

CREATE TABLE `float_table` (
    `id` INT64,
    `float_col` FLOAT64
) PRIMARY KEY (`id`);

CREATE TABLE `real_table` (
    `id` INT64,
    `real_col` FLOAT32
) PRIMARY KEY (`id`);

CREATE TABLE `date_table` (
    `id` INT64,
    `date_col` DATE
) PRIMARY KEY (`id`);

CREATE TABLE `time_table` (
    `id` INT64,
    `time_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `datetime2_table` (
    `id` INT64,
    `datetime2_col` TIMESTAMP
) PRIMARY KEY (`id`);

CREATE TABLE `datetimeoffset_table` (
    `id` INT64,
    `datetimeoffset_col` TIMESTAMP
) PRIMARY KEY (`id`);

CREATE TABLE `datetime_table` (
    `id` INT64,
    `datetime_col` TIMESTAMP
) PRIMARY KEY (`id`);

CREATE TABLE `smalldatetime_table` (
    `id` INT64,
    `smalldatetime_col` TIMESTAMP
) PRIMARY KEY (`id`);

CREATE TABLE `char_table` (
    `id` INT64,
    `char_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `text_table` (
    `id` INT64,
    `text_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `nchar_table` (
    `id` INT64,
    `nchar_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `nvarchar_table` (
    `id` INT64,
    `nvarchar_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `ntext_table` (
    `id` INT64,
    `ntext_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `binary_table` (
    `id` INT64,
    `binary_col` BYTES(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `varbinary_table` (
    `id` INT64,
    `varbinary_col` BYTES(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `image_table` (
    `id` INT64,
    `image_col` BYTES(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `uniqueidentifier_table` (
    `id` INT64,
    `uniqueidentifier_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `xml_table` (
    `id` INT64,
    `xml_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `tinyint_to_string_table` (
    `id` INT64,
    `tinyint_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `smallint_to_string_table` (
    `id` INT64,
    `smallint_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `int_to_string_table` (
    `id` INT64,
    `int_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `bigint_to_string_table` (
    `id` INT64,
    `bigint_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `bit_to_int64_table` (
    `id` INT64,
    `bit_to_int64_col` INT64
) PRIMARY KEY (`id`);

CREATE TABLE `bit_to_string_table` (
    `id` INT64,
    `bit_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `decimal_to_string_table` (
    `id` INT64,
    `decimal_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `decimal_to_float64_table` (
    `id` INT64,
    `decimal_to_float64_col` FLOAT64
) PRIMARY KEY (`id`);

CREATE TABLE `numeric_to_string_table` (
    `id` INT64,
    `numeric_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `money_to_string_table` (
    `id` INT64,
    `money_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `smallmoney_to_string_table` (
    `id` INT64,
    `smallmoney_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `float_to_string_table` (
    `id` INT64,
    `float_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `real_to_float64_table` (
    `id` INT64,
    `real_to_float64_col` FLOAT64
) PRIMARY KEY (`id`);

CREATE TABLE `real_to_string_table` (
    `id` INT64,
    `real_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `date_to_string_table` (
    `id` INT64,
    `date_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `datetime2_to_string_table` (
    `id` INT64,
    `datetime2_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `datetimeoffset_to_string_table` (
    `id` INT64,
    `datetimeoffset_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `datetime_to_string_table` (
    `id` INT64,
    `datetime_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `smalldatetime_to_string_table` (
    `id` INT64,
    `smalldatetime_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `char_to_bytes_table` (
    `id` INT64,
    `char_to_bytes_col` BYTES(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `varchar_to_bytes_table` (
    `id` INT64,
    `varchar_to_bytes_col` BYTES(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `nchar_to_bytes_table` (
    `id` INT64,
    `nchar_to_bytes_col` BYTES(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `nvarchar_to_bytes_table` (
    `id` INT64,
    `nvarchar_to_bytes_col` BYTES(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `binary_to_string_table` (
    `id` INT64,
    `binary_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `varbinary_to_string_table` (
    `id` INT64,
    `varbinary_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `image_to_string_table` (
    `id` INT64,
    `image_to_string_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `tinyint_pk_table` (
    `id` INT64,
    `tinyint_pk_col` INT64
) PRIMARY KEY (`id`);

CREATE TABLE `smallint_pk_table` (
    `id` INT64,
    `smallint_pk_col` INT64
) PRIMARY KEY (`id`);

CREATE TABLE `int_pk_table` (
    `id` INT64,
    `int_pk_col` INT64
) PRIMARY KEY (`id`);

CREATE TABLE `bigint_pk_table` (
    `id` INT64,
    `bigint_pk_col` INT64
) PRIMARY KEY (`id`);

CREATE TABLE `bit_pk_table` (
    `id` BOOL,
    `bit_pk_col` BOOL
) PRIMARY KEY (`id`);

CREATE TABLE `date_pk_table` (
    `id` DATE,
    `date_pk_col` DATE
) PRIMARY KEY (`id`);

CREATE TABLE `time_pk_table` (
    `id` STRING(MAX),
    `time_pk_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `datetime2_pk_table` (
    `id` TIMESTAMP,
    `datetime2_pk_col` TIMESTAMP
) PRIMARY KEY (`id`);

CREATE TABLE `datetimeoffset_pk_table` (
    `id` TIMESTAMP,
    `datetimeoffset_pk_col` TIMESTAMP
) PRIMARY KEY (`id`);

CREATE TABLE `datetime_pk_table` (
    `id` TIMESTAMP,
    `datetime_pk_col` TIMESTAMP
) PRIMARY KEY (`id`);

CREATE TABLE `smalldatetime_pk_table` (
    `id` TIMESTAMP,
    `smalldatetime_pk_col` TIMESTAMP
) PRIMARY KEY (`id`);

CREATE TABLE `char_pk_table` (
    `id` STRING(100),
    `char_pk_col` STRING(100)
) PRIMARY KEY (`id`);

CREATE TABLE `varchar_pk_table` (
    `id` STRING(100),
    `varchar_pk_col` STRING(100)
) PRIMARY KEY (`id`);

CREATE TABLE `nchar_pk_table` (
    `id` STRING(100),
    `nchar_pk_col` STRING(100)
) PRIMARY KEY (`id`);

CREATE TABLE `nvarchar_pk_table` (
    `id` STRING(100),
    `nvarchar_pk_col` STRING(100)
) PRIMARY KEY (`id`);

CREATE TABLE `binary_pk_table` (
    `id` BYTES(100),
    `binary_pk_col` BYTES(100)
) PRIMARY KEY (`id`);

CREATE TABLE `varbinary_pk_table` (
    `id` BYTES(100),
    `varbinary_pk_col` BYTES(100)
) PRIMARY KEY (`id`);

CREATE TABLE `uniqueidentifier_pk_table` (
    `id` STRING(MAX),
    `uniqueidentifier_pk_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `generated_pk_column` (
    `first_name_col` STRING(MAX),
    `last_name_col` STRING(MAX),
    `generated_column_col` STRING(MAX)
) PRIMARY KEY (`generated_column_col`);

CREATE TABLE `generated_non_pk_column` (
    `id` INT64,
    `first_name_col` STRING(MAX),
    `last_name_col` STRING(MAX),
    `generated_column_col` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `non_generated_to_generated_column` (
    `first_name_col` STRING(MAX),
    `last_name_col` STRING(MAX),
    `generated_column_col` STRING(MAX),
    `generated_column_pk_col` STRING(MAX)
) PRIMARY KEY (`generated_column_pk_col`);

CREATE TABLE `generated_to_non_generated_column` (
    `first_name_col` STRING(MAX),
    `last_name_col` STRING(MAX),
    `generated_column_col` STRING(MAX),
    `generated_column_pk_col` STRING(MAX)
) PRIMARY KEY (`generated_column_pk_col`);
