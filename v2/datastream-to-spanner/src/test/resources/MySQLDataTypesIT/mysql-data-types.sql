CREATE TABLE `varchar_table` (
    `id` INT PRIMARY KEY,
    `varchar_col` VARCHAR(21000) CHARACTER SET utf8 DEFAULT NULL
);

CREATE TABLE `tinyint_table` (
    `id` INT PRIMARY KEY,
    `tinyint_col` TINYINT DEFAULT NULL
);

CREATE TABLE `tinyint_unsigned_table` (
    `id` INT PRIMARY KEY,
    `tinyint_unsigned_col` TINYINT UNSIGNED DEFAULT NULL
);

CREATE TABLE `text_table` (
    `id` INT PRIMARY KEY,
    `text_col` TEXT CHARACTER SET utf8 DEFAULT NULL
);

CREATE TABLE `date_table` (
    `id` INT PRIMARY KEY,
    `date_col` DATE DEFAULT NULL
);

CREATE TABLE `smallint_table` (
    `id` INT PRIMARY KEY,
    `smallint_col` SMALLINT DEFAULT NULL
);

CREATE TABLE `smallint_unsigned_table` (
    `id` INT PRIMARY KEY,
    `smallint_unsigned_col` SMALLINT UNSIGNED DEFAULT NULL
);

CREATE TABLE `mediumint_table` (
    `id` INT PRIMARY KEY,
    `mediumint_col` MEDIUMINT DEFAULT NULL
);

CREATE TABLE `mediumint_unsigned_table` (
    `id` INT PRIMARY KEY,
    `mediumint_unsigned_col` MEDIUMINT UNSIGNED DEFAULT NULL
);
CREATE TABLE `bigint_table` (
    `id` INT PRIMARY KEY,
    `bigint_col` BIGINT DEFAULT NULL
);

CREATE TABLE `bigint_unsigned_table` (
     `id` INT PRIMARY KEY,
     `bigint_unsigned_col` BIGINT UNSIGNED DEFAULT NULL
);

CREATE TABLE `float_table` (
    `id` INT PRIMARY KEY,
    `float_col` FLOAT DEFAULT NULL
);

CREATE TABLE `double_table` (
    `id` INT PRIMARY KEY,
    `double_col` DOUBLE DEFAULT NULL
);

CREATE TABLE `decimal_table` (
    `id` INT PRIMARY KEY,
    `decimal_col` DECIMAL(65,30) DEFAULT NULL
);

CREATE TABLE `datetime_table` (
    `id` INT PRIMARY KEY,
    `datetime_col` DATETIME DEFAULT NULL
);

CREATE TABLE `time_table` (
    `id` INT PRIMARY KEY,
    `time_col` TIME DEFAULT NULL
);

CREATE TABLE `year_table` (
    `id` INT PRIMARY KEY,
    `year_col` YEAR DEFAULT NULL
);

CREATE TABLE `char_table` (
    `id` INT PRIMARY KEY,
    `char_col` CHAR(255) CHARACTER SET utf8 DEFAULT NULL
);

CREATE TABLE `tinyblob_table` (
    `id` INT PRIMARY KEY,
    `tinyblob_col` TINYBLOB DEFAULT NULL
);

CREATE TABLE `tinytext_table` (
    `id` INT PRIMARY KEY,
    `tinytext_col` TINYTEXT CHARACTER SET utf8 DEFAULT NULL
);

CREATE TABLE `blob_table` (
    `id` INT PRIMARY KEY,
    `blob_col` BLOB DEFAULT NULL
);

CREATE TABLE `mediumblob_table` (
    `id` INT PRIMARY KEY,
    `mediumblob_col` MEDIUMBLOB DEFAULT NULL
);

CREATE TABLE `mediumtext_table` (
    `id` INT PRIMARY KEY,
    `mediumtext_col` MEDIUMTEXT CHARACTER SET utf8 DEFAULT NULL
);

CREATE TABLE `test_json_table` (
    `id` INT PRIMARY KEY,
    `test_json_col` JSON DEFAULT NULL
);

CREATE TABLE `longblob_table` (
    `id` INT PRIMARY KEY,
    `longblob_col` LONGBLOB DEFAULT NULL
);

CREATE TABLE `longtext_table` (
    `id` INT PRIMARY KEY,
    `longtext_col` LONGTEXT CHARACTER SET utf8 DEFAULT NULL
);

CREATE TABLE `enum_table` (
    `id` INT PRIMARY KEY,
    `enum_col` ENUM('1','2','3') CHARACTER SET utf8 DEFAULT NULL
);

CREATE TABLE `bool_table` (
    `id` INT PRIMARY KEY,
    `bool_col` TINYINT(1) DEFAULT NULL
);

CREATE TABLE `binary_table` (
    `id` INT PRIMARY KEY,
    `binary_col` BINARY(255) DEFAULT NULL
);

CREATE TABLE `varbinary_table` (
    `id` INT PRIMARY KEY,
    `varbinary_col` VARBINARY(65000) DEFAULT NULL
);

CREATE TABLE `bit_table` (
    `id` INT PRIMARY KEY,
    `bit_col` BIT(64) DEFAULT NULL
);

CREATE TABLE `boolean_table` (
    `id` INT PRIMARY KEY,
    `boolean_col` TINYINT(1) DEFAULT NULL
);

CREATE TABLE `int_table` (
    `id` INT PRIMARY KEY,
    `int_col` INT DEFAULT NULL
);

CREATE TABLE `integer_unsigned_table` (
    `id` INT PRIMARY KEY,
    `integer_unsigned_col` INTEGER UNSIGNED DEFAULT NULL
);

CREATE TABLE `timestamp_table` (
    `id` INT PRIMARY KEY,
    `timestamp_col` TIMESTAMP DEFAULT NULL
);

CREATE TABLE set_table (
    id INT PRIMARY KEY,
    set_col SET('v1', 'v2', 'v3') DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS bit_to_bool_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  bit_to_bool_col BIT
);

CREATE TABLE IF NOT EXISTS bit_to_int64_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  bit_to_int64_col BIT(64)
);

CREATE TABLE IF NOT EXISTS bit_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  bit_to_string_col BIT(16)
);

CREATE TABLE IF NOT EXISTS bool_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  bool_to_string_col BOOL
);

CREATE TABLE IF NOT EXISTS boolean_to_bool_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  boolean_to_bool_col BOOLEAN
);

CREATE TABLE IF NOT EXISTS boolean_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  boolean_to_string_col BOOLEAN
);

CREATE TABLE IF NOT EXISTS tinyint_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  tinyint_to_string_col TINYINT
);

CREATE TABLE IF NOT EXISTS smallint_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  smallint_to_string_col SMALLINT
);

CREATE TABLE IF NOT EXISTS mediumint_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  mediumint_to_string_col MEDIUMINT
);

CREATE TABLE IF NOT EXISTS int_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  int_to_string_col INT
);

CREATE TABLE IF NOT EXISTS integer_to_int64_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  integer_to_int64_col INTEGER
);

CREATE TABLE IF NOT EXISTS integer_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  integer_to_string_col INTEGER
);

CREATE TABLE IF NOT EXISTS bigint_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  bigint_to_string_col BIGINT
);

CREATE TABLE IF NOT EXISTS decimal_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  decimal_to_string_col DECIMAL(65, 30)
);

CREATE TABLE IF NOT EXISTS dec_to_numeric_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  dec_to_numeric_col DEC(65, 30)
);

CREATE TABLE IF NOT EXISTS dec_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  dec_to_string_col DEC(65, 30)
);

CREATE TABLE IF NOT EXISTS numeric_to_numeric_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  numeric_to_numeric_col NUMERIC(65, 30)
);

CREATE TABLE IF NOT EXISTS numeric_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  numeric_to_string_col NUMERIC(65, 30)
);

CREATE TABLE IF NOT EXISTS float_to_float32_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  float_to_float32_col FLOAT
);

CREATE TABLE IF NOT EXISTS float_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  float_to_string_col FLOAT
);

CREATE TABLE IF NOT EXISTS double_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  double_to_string_col DOUBLE
);

CREATE TABLE IF NOT EXISTS double_precision_to_float64_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  double_precision_to_float64_col DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS double_precision_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  double_precision_to_string_col DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS real_to_float64_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  real_to_float64_col REAL
);

CREATE TABLE IF NOT EXISTS real_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  real_to_string_col REAL
);

CREATE TABLE IF NOT EXISTS date_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  date_to_string_col DATE
);

CREATE TABLE IF NOT EXISTS datetime_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  datetime_to_string_col DATETIME
);

CREATE TABLE IF NOT EXISTS timestamp_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  timestamp_to_string_col TIMESTAMP
);

CREATE TABLE IF NOT EXISTS binary_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  binary_to_string_col BINARY(255)
);

CREATE TABLE IF NOT EXISTS varbinary_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  varbinary_to_string_col VARBINARY(65000)
);

CREATE TABLE IF NOT EXISTS tinyblob_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  tinyblob_to_string_col TINYBLOB
);

CREATE TABLE IF NOT EXISTS blob_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  blob_to_string_col BLOB
);

CREATE TABLE IF NOT EXISTS mediumblob_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  mediumblob_to_string_col MEDIUMBLOB
);

CREATE TABLE IF NOT EXISTS longblob_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  longblob_to_string_col LONGBLOB
);

CREATE TABLE IF NOT EXISTS set_to_array_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  set_to_array_col SET('v1', 'v2', 'v3')
);

CREATE TABLE IF NOT EXISTS json_to_string_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  json_to_string_col JSON
);

CREATE TABLE IF NOT EXISTS `generated_pk_column_table` ( 
	`first_name_col` varchar(50) DEFAULT NULL,
	`last_name_col` varchar(50) DEFAULT NULL,
	`generated_column_col` varchar(100) GENERATED ALWAYS AS (concat(`first_name_col`,' ')) STORED NOT NULL,
	PRIMARY KEY (`generated_column_col`)
);

CREATE TABLE IF NOT EXISTS `generated_non_pk_column_table` ( 
	`first_name_col` varchar(50) DEFAULT NULL,
	`last_name_col` varchar(50) DEFAULT NULL,
	`generated_column_col` varchar(100) GENERATED ALWAYS AS (concat(`first_name_col`,' ')) STORED NOT NULL,
  `id` int not null,
	PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `non_generated_to_generated_column_table` ( 
	`first_name_col` varchar(50) DEFAULT NULL,
	`last_name_col` varchar(50) DEFAULT NULL,
  `generated_column_col` varchar(100) NOT NULL,
	`generated_column_pk_col` varchar(100) NOT NULL,
	PRIMARY KEY (`generated_column_pk_col`)
);

CREATE TABLE IF NOT EXISTS `generated_to_non_generated_column_table` ( 
	`first_name_col` varchar(50) DEFAULT NULL,
	`last_name_col` varchar(50) DEFAULT NULL,
  `generated_column_col` varchar(100) GENERATED ALWAYS AS (concat(`first_name_col`,' ')) STORED NOT NULL,
	`generated_column_pk_col` varchar(100) GENERATED ALWAYS AS (concat(`first_name_col`,' ')) STORED NOT NULL,
	PRIMARY KEY (`generated_column_pk_col`)
);

ALTER TABLE `bigint_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `bigint_unsigned_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `binary_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `bit_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `blob_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `bool_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `boolean_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `char_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `date_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `datetime_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `decimal_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `double_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `enum_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `float_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `int_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `integer_unsigned_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `test_json_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `longblob_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `longtext_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `mediumblob_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `mediumint_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `mediumint_unsigned_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `mediumtext_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `smallint_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `smallint_unsigned_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `text_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `time_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `timestamp_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `tinyblob_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `tinyint_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `tinyint_unsigned_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `tinytext_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `varbinary_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `varchar_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `year_table` MODIFY `id` INT AUTO_INCREMENT;
ALTER TABLE `set_table` MODIFY `id` INT AUTO_INCREMENT;

INSERT INTO `bigint_table` (`bigint_col`) VALUES (40);
INSERT INTO `bigint_table` (`bigint_col`) VALUES (9223372036854775807);
INSERT INTO `bigint_table` (`bigint_col`) VALUES (-9223372036854775808);
INSERT INTO `bigint_to_string_table` (`bigint_to_string_col`) VALUES (40);
INSERT INTO `bigint_to_string_table` (`bigint_to_string_col`) VALUES (9223372036854775807);
INSERT INTO `bigint_to_string_table` (`bigint_to_string_col`) VALUES (-9223372036854775808);
INSERT INTO `bigint_unsigned_table` (`bigint_unsigned_col`) VALUES (42);
INSERT INTO `bigint_unsigned_table` (`bigint_unsigned_col`) VALUES (0);
INSERT INTO `bigint_unsigned_table` (`bigint_unsigned_col`) VALUES (18446744073709551615);
INSERT INTO `binary_table` (`binary_col`) VALUES (x'7835383030000000000000000000000000000000');
INSERT INTO `binary_table` (`binary_col`) VALUES (REPEAT(X'FF', 255));
INSERT INTO `binary_to_string_table` (`binary_to_string_col`) VALUES (x'7835383030000000000000000000000000000000');
INSERT INTO `binary_to_string_table` (`binary_to_string_col`) VALUES (REPEAT(X'FF', 255));
INSERT INTO `bit_table` (`bit_col`) VALUES (b'0111111111111111111111111111111111111111111111111111111111111111');
INSERT INTO `bit_to_bool_table` (`bit_to_bool_col`) VALUES (b'0');
INSERT INTO `bit_to_bool_table` (`bit_to_bool_col`) VALUES (b'1');
INSERT INTO `bit_to_string_table` (`bit_to_string_col`) VALUES (b'0111111111111111');
INSERT INTO `bit_to_int64_table` (`bit_to_int64_col`) VALUES (b'0111111111111111111111111111111111111111111111111111111111111111');
INSERT INTO `blob_table` (`blob_col`) VALUES (X'7835383030');
INSERT INTO `blob_table` (`blob_col`) VALUES (REPEAT(X'FF', 65535));
INSERT INTO `blob_to_string_table` (`blob_to_string_col`) VALUES (X'7835383030');
INSERT INTO `blob_to_string_table` (`blob_to_string_col`) VALUES (REPEAT(X'FF', 65535));
INSERT INTO `bool_table` (`bool_col`) VALUES (0);
INSERT INTO `bool_table` (`bool_col`) VALUES (1);
INSERT INTO `bool_to_string_table` (`bool_to_string_col`) VALUES (0);
INSERT INTO `bool_to_string_table` (`bool_to_string_col`) VALUES (1);
INSERT INTO `boolean_table` (`boolean_col`) VALUES (0);
INSERT INTO `boolean_table` (`boolean_col`) VALUES (1);
INSERT INTO `boolean_to_bool_table` (`boolean_to_bool_col`) VALUES (0);
INSERT INTO `boolean_to_bool_table` (`boolean_to_bool_col`) VALUES (1);
INSERT INTO `boolean_to_string_table` (`boolean_to_string_col`) VALUES (0);
INSERT INTO `boolean_to_string_table` (`boolean_to_string_col`) VALUES (1);
INSERT INTO `char_table` (`char_col`) VALUES ('a');
INSERT INTO `char_table` (`char_col`) VALUES (REPEAT('a', 255));
INSERT INTO `date_table` (`date_col`) VALUES ('2012-09-17');
INSERT INTO `date_table` (`date_col`) VALUES ('1000-01-01');
INSERT INTO `date_table` (`date_col`) VALUES ('9999-12-31');
INSERT INTO `date_to_string_table` (`date_to_string_col`) VALUES ('2012-09-17');
INSERT INTO `date_to_string_table` (`date_to_string_col`) VALUES ('1000-01-01');
INSERT INTO `date_to_string_table` (`date_to_string_col`) VALUES ('9999-12-31');
INSERT INTO `datetime_table` (`datetime_col`) VALUES ('1998-01-23 12:45:56');
INSERT INTO `datetime_table` (`datetime_col`) VALUES ('1000-01-01 00:00:00');
INSERT INTO `datetime_table` (`datetime_col`) VALUES ('9999-12-31 23:59:59');
INSERT INTO `datetime_to_string_table` (`datetime_to_string_col`) VALUES ('1998-01-23 12:45:56');
INSERT INTO `datetime_to_string_table` (`datetime_to_string_col`) VALUES ('1000-01-01 00:00:00');
INSERT INTO `datetime_to_string_table` (`datetime_to_string_col`) VALUES ('9999-12-31 23:59:59');
INSERT INTO `dec_to_numeric_table` (`dec_to_numeric_col`) VALUES (68.75);
INSERT INTO `dec_to_numeric_table` (`dec_to_numeric_col`) VALUES (99999999999999999999999.999999999);
INSERT INTO `dec_to_numeric_table` (`dec_to_numeric_col`) VALUES (12345678912345678.123456789012345678912452300000);
INSERT INTO `dec_to_string_table` (`dec_to_string_col`) VALUES (68.75);
INSERT INTO `dec_to_string_table` (`dec_to_string_col`) VALUES (99999999999999999999999.999999999);
INSERT INTO `dec_to_string_table` (`dec_to_string_col`) VALUES (12345678912345678.123456789012345678912452300000);
INSERT INTO `decimal_table` (`decimal_col`) VALUES (68.75);
INSERT INTO `decimal_table` (`decimal_col`) VALUES (99999999999999999999999.999999999);
INSERT INTO `decimal_table` (`decimal_col`) VALUES (12345678912345678.123456789012345678912452300000);
INSERT INTO `decimal_to_string_table` (`decimal_to_string_col`) VALUES (68.75);
INSERT INTO `decimal_to_string_table` (`decimal_to_string_col`) VALUES (99999999999999999999999.999999999);
INSERT INTO `decimal_to_string_table` (`decimal_to_string_col`) VALUES (12345678912345678.123456789012345678912452300000);
INSERT INTO `double_precision_to_float64_table` (`double_precision_to_float64_col`) VALUES (52.67);
INSERT INTO `double_precision_to_float64_table` (`double_precision_to_float64_col`) VALUES (1.7976931348623157E308);
INSERT INTO `double_precision_to_float64_table` (`double_precision_to_float64_col`) VALUES (-1.7976931348623157E308);
INSERT INTO `double_precision_to_string_table` (`double_precision_to_string_col`) VALUES (52.67);
INSERT INTO `double_precision_to_string_table` (`double_precision_to_string_col`) VALUES (1.7976931348623157E308);
INSERT INTO `double_precision_to_string_table` (`double_precision_to_string_col`) VALUES (-1.7976931348623157E308);
INSERT INTO `double_table` (`double_col`) VALUES (52.67);
INSERT INTO `double_table` (`double_col`) VALUES (1.7976931348623157E308);
INSERT INTO `double_table` (`double_col`) VALUES (-1.7976931348623157E308);
INSERT INTO `double_to_string_table` (`double_to_string_col`) VALUES (52.67);
INSERT INTO `double_to_string_table` (`double_to_string_col`) VALUES (1.7976931348623157E308);
INSERT INTO `double_to_string_table` (`double_to_string_col`) VALUES (-1.7976931348623157E308);
INSERT INTO `enum_table` (`enum_col`) VALUES ('1');
INSERT INTO `float_table` (`float_col`) VALUES (45.56);
INSERT INTO `float_table` (`float_col`) VALUES (3.4E38);
INSERT INTO `float_table` (`float_col`) VALUES (-3.4E38);
INSERT INTO `float_to_float32_table` (`float_to_float32_col`) VALUES (45.56);
INSERT INTO `float_to_float32_table` (`float_to_float32_col`) VALUES (3.4E38);
INSERT INTO `float_to_float32_table` (`float_to_float32_col`) VALUES (-3.4E38);
INSERT INTO `float_to_string_table` (`float_to_string_col`) VALUES (45.56);
INSERT INTO `float_to_string_table` (`float_to_string_col`) VALUES (3.4E38);
INSERT INTO `float_to_string_table` (`float_to_string_col`) VALUES (-3.4E38);
INSERT INTO `int_table` (`int_col`) VALUES (30);
INSERT INTO `int_table` (`int_col`) VALUES (2147483647);
INSERT INTO `int_table` (`int_col`) VALUES (-2147483648);
INSERT INTO `int_to_string_table` (`int_to_string_col`) VALUES (30);
INSERT INTO `int_to_string_table` (`int_to_string_col`) VALUES (2147483647);
INSERT INTO `int_to_string_table` (`int_to_string_col`) VALUES (-2147483648);
INSERT INTO `integer_to_int64_table` (`integer_to_int64_col`) VALUES (30);
INSERT INTO `integer_to_int64_table` (`integer_to_int64_col`) VALUES (2147483647);
INSERT INTO `integer_to_int64_table` (`integer_to_int64_col`) VALUES (-2147483648);
INSERT INTO `integer_to_string_table` (`integer_to_string_col`) VALUES (30);
INSERT INTO `integer_to_string_table` (`integer_to_string_col`) VALUES (2147483647);
INSERT INTO `integer_to_string_table` (`integer_to_string_col`) VALUES (-2147483648);
INSERT INTO `integer_unsigned_table` (`integer_unsigned_col`) VALUES (0);
INSERT INTO `integer_unsigned_table` (`integer_unsigned_col`) VALUES (42);
INSERT INTO `integer_unsigned_table` (`integer_unsigned_col`) VALUES (4294967295);
INSERT INTO `test_json_table` (`test_json_col`) VALUES ('{"k1":"v1"}');
INSERT INTO `json_to_string_table` (`json_to_string_col`) VALUES ('{"k1":"v1"}');
INSERT INTO `longblob_table` (`longblob_col`) VALUES (X'7835383030');
INSERT INTO `longblob_table` (`longblob_col`) VALUES (REPEAT(X'FF', 65535));
INSERT INTO `longblob_to_string_table` (`longblob_to_string_col`) VALUES (X'7835383030');
INSERT INTO `longblob_to_string_table` (`longblob_to_string_col`) VALUES (REPEAT(X'FF', 65535));
INSERT INTO `longtext_table` (`longtext_col`) VALUES ('longtext');
INSERT INTO `longtext_table` (`longtext_col`) VALUES (REPEAT('a', 65535));
INSERT INTO `mediumblob_table` (`mediumblob_col`) VALUES (X'7835383030');
INSERT INTO `mediumblob_table` (`mediumblob_col`) VALUES (REPEAT(X'FF', 65535));
INSERT INTO `mediumblob_to_string_table` (`mediumblob_to_string_col`) VALUES (X'7835383030');
INSERT INTO `mediumblob_to_string_table` (`mediumblob_to_string_col`) VALUES (REPEAT(X'FF', 65535));
INSERT INTO `mediumint_table` (`mediumint_col`) VALUES (20);
INSERT INTO `mediumint_to_string_table` (`mediumint_to_string_col`) VALUES (20);
INSERT INTO `mediumint_unsigned_table` (`mediumint_unsigned_col`) VALUES (42);
INSERT INTO `mediumint_unsigned_table` (`mediumint_unsigned_col`) VALUES (0);
INSERT INTO `mediumint_unsigned_table` (`mediumint_unsigned_col`) VALUES (16777215);
INSERT INTO `mediumtext_table` (`mediumtext_col`) VALUES ('mediumtext');
INSERT INTO `mediumtext_table` (`mediumtext_col`) VALUES (REPEAT('a', 65535));
INSERT INTO `numeric_to_numeric_table` (`numeric_to_numeric_col`) VALUES (68.75);
INSERT INTO `numeric_to_numeric_table` (`numeric_to_numeric_col`) VALUES (99999999999999999999999.999999999);
INSERT INTO `numeric_to_numeric_table` (`numeric_to_numeric_col`) VALUES (12345678912345678.123456789012345678912452300000);
INSERT INTO `numeric_to_string_table` (`numeric_to_string_col`) VALUES (68.75);
INSERT INTO `numeric_to_string_table` (`numeric_to_string_col`) VALUES (99999999999999999999999.999999999);
INSERT INTO `numeric_to_string_table` (`numeric_to_string_col`) VALUES (12345678912345678.123456789012345678912452300000);
INSERT INTO `real_to_float64_table` (`real_to_float64_col`) VALUES (52.67);
INSERT INTO `real_to_float64_table` (`real_to_float64_col`) VALUES (1.7976931348623157E308);
INSERT INTO `real_to_float64_table` (`real_to_float64_col`) VALUES (-1.7976931348623157E308);
INSERT INTO `real_to_string_table` (`real_to_string_col`) VALUES (52.67);
INSERT INTO `real_to_string_table` (`real_to_string_col`) VALUES (1.7976931348623157E308);
INSERT INTO `real_to_string_table` (`real_to_string_col`) VALUES (-1.7976931348623157E308);
INSERT INTO `set_to_array_table` (`set_to_array_col`) VALUES ('v1,v2');
INSERT INTO `smallint_table` (`smallint_col`) VALUES (15);
INSERT INTO `smallint_table` (`smallint_col`) VALUES (32767);
INSERT INTO `smallint_table` (`smallint_col`) VALUES (-32768);
INSERT INTO `smallint_to_string_table` (`smallint_to_string_col`) VALUES (15);
INSERT INTO `smallint_to_string_table` (`smallint_to_string_col`) VALUES (32767);
INSERT INTO `smallint_to_string_table` (`smallint_to_string_col`) VALUES (-32768);
INSERT INTO `smallint_unsigned_table` (`smallint_unsigned_col`) VALUES (42);
INSERT INTO `smallint_unsigned_table` (`smallint_unsigned_col`) VALUES (0);
INSERT INTO `smallint_unsigned_table` (`smallint_unsigned_col`) VALUES (65535);
INSERT INTO `text_table` (`text_col`) VALUES ('xyz');
INSERT INTO `text_table` (`text_col`) VALUES (REPEAT('a', 65535));
INSERT INTO `time_table` (`time_col`) VALUES ('15:50:00');
INSERT INTO `time_table` (`time_col`) VALUES ('838:59:59');
INSERT INTO `time_table` (`time_col`) VALUES ('-838:59:59');
INSERT INTO `timestamp_table` (`timestamp_col`) VALUES ('2022-08-05 08:23:11');
INSERT INTO `timestamp_table` (`timestamp_col`) VALUES ('1970-01-01 00:00:01');
INSERT INTO `timestamp_table` (`timestamp_col`) VALUES ('2038-01-19 03:14:07');
INSERT INTO `timestamp_to_string_table` (`timestamp_to_string_col`) VALUES ('2022-08-05 08:23:11');
INSERT INTO `timestamp_to_string_table` (`timestamp_to_string_col`) VALUES ('1970-01-01 00:00:01');
INSERT INTO `timestamp_to_string_table` (`timestamp_to_string_col`) VALUES ('2038-01-19 03:14:07');
INSERT INTO `tinyblob_table` (`tinyblob_col`) VALUES (X'7835383030');
INSERT INTO `tinyblob_table` (`tinyblob_col`) VALUES (REPEAT(X'FF', 255));
INSERT INTO `tinyblob_to_string_table` (`tinyblob_to_string_col`) VALUES (X'7835383030');
INSERT INTO `tinyblob_to_string_table` (`tinyblob_to_string_col`) VALUES (REPEAT(X'FF', 255));
INSERT INTO `tinyint_table` (`tinyint_col`) VALUES (10);
INSERT INTO `tinyint_table` (`tinyint_col`) VALUES (127);
INSERT INTO `tinyint_table` (`tinyint_col`) VALUES (-128);
INSERT INTO `tinyint_to_string_table` (`tinyint_to_string_col`) VALUES (10);
INSERT INTO `tinyint_to_string_table` (`tinyint_to_string_col`) VALUES (127);
INSERT INTO `tinyint_to_string_table` (`tinyint_to_string_col`) VALUES (-128);
INSERT INTO `tinyint_unsigned_table` (`tinyint_unsigned_col`) VALUES (0);
INSERT INTO `tinyint_unsigned_table` (`tinyint_unsigned_col`) VALUES (255);
INSERT INTO `tinytext_table` (`tinytext_col`) VALUES ('tinytext');
INSERT INTO `tinytext_table` (`tinytext_col`) VALUES (REPEAT('a', 255));
INSERT INTO `varbinary_table` (`varbinary_col`) VALUES (X'7835383030');
INSERT INTO `varbinary_table` (`varbinary_col`) VALUES (REPEAT(X'FF', 65000));
INSERT INTO `varbinary_to_string_table` (`varbinary_to_string_col`) VALUES (X'7835383030');
INSERT INTO `varbinary_to_string_table` (`varbinary_to_string_col`) VALUES (REPEAT(X'FF', 65000));
INSERT INTO `varchar_table` (`varchar_col`) VALUES ('abc');
INSERT INTO `varchar_table` (`varchar_col`) VALUES (REPEAT('a', 21000));
INSERT INTO `year_table` (`year_col`) VALUES (2022);
INSERT INTO `year_table` (`year_col`) VALUES (1901);
INSERT INTO `year_table` (`year_col`) VALUES (2155);
INSERT INTO `set_table` (`set_col`) VALUES ('v1,v2');

INSERT INTO `bigint_table` (`bigint_col`) VALUES (NULL);
INSERT INTO `bigint_to_string_table` (`bigint_to_string_col`) VALUES (NULL);
INSERT INTO `bigint_unsigned_table` (`bigint_unsigned_col`) VALUES (NULL);
INSERT INTO `binary_table` (`binary_col`) VALUES (NULL);
INSERT INTO `binary_to_string_table` (`binary_to_string_col`) VALUES (NULL);
INSERT INTO `bit_table` (`bit_col`) VALUES (NULL);
INSERT INTO `bit_to_bool_table` (`bit_to_bool_col`) VALUES (NULL);
INSERT INTO `bit_to_string_table` (`bit_to_string_col`) VALUES (NULL);
INSERT INTO `bit_to_int64_table` (`bit_to_int64_col`) VALUES (NULL);
INSERT INTO `blob_table` (`blob_col`) VALUES (NULL);
INSERT INTO `blob_to_string_table` (`blob_to_string_col`) VALUES (NULL);
INSERT INTO `bool_table` (`bool_col`) VALUES (NULL);
INSERT INTO `bool_to_string_table` (`bool_to_string_col`) VALUES (NULL);
INSERT INTO `boolean_table` (`boolean_col`) VALUES (NULL);
INSERT INTO `boolean_to_bool_table` (`boolean_to_bool_col`) VALUES (NULL);
INSERT INTO `boolean_to_string_table` (`boolean_to_string_col`) VALUES (NULL);
INSERT INTO `char_table` (`char_col`) VALUES (NULL);
INSERT INTO `date_table` (`date_col`) VALUES (NULL);
INSERT INTO `date_to_string_table` (`date_to_string_col`) VALUES (NULL);
INSERT INTO `datetime_table` (`datetime_col`) VALUES (NULL);
INSERT INTO `datetime_to_string_table` (`datetime_to_string_col`) VALUES (NULL);
INSERT INTO `dec_to_numeric_table` (`dec_to_numeric_col`) VALUES (NULL);
INSERT INTO `dec_to_string_table` (`dec_to_string_col`) VALUES (NULL);
INSERT INTO `decimal_table` (`decimal_col`) VALUES (NULL);
INSERT INTO `decimal_to_string_table` (`decimal_to_string_col`) VALUES (NULL);
INSERT INTO `double_precision_to_float64_table` (`double_precision_to_float64_col`) VALUES (NULL);
INSERT INTO `double_precision_to_string_table` (`double_precision_to_string_col`) VALUES (NULL);
INSERT INTO `double_table` (`double_col`) VALUES (NULL);
INSERT INTO `double_to_string_table` (`double_to_string_col`) VALUES (NULL);
INSERT INTO `enum_table` (`enum_col`) VALUES (NULL);
INSERT INTO `float_table` (`float_col`) VALUES (NULL);
INSERT INTO `float_to_float32_table` (`float_to_float32_col`) VALUES (NULL);
INSERT INTO `float_to_string_table` (`float_to_string_col`) VALUES (NULL);
INSERT INTO `int_table` (`int_col`) VALUES (NULL);
INSERT INTO `int_to_string_table` (`int_to_string_col`) VALUES (NULL);
INSERT INTO `integer_to_int64_table` (`integer_to_int64_col`) VALUES (NULL);
INSERT INTO `integer_to_string_table` (`integer_to_string_col`) VALUES (NULL);
INSERT INTO `integer_unsigned_table` (`integer_unsigned_col`) VALUES (NULL);
INSERT INTO `test_json_table` (`test_json_col`) VALUES (NULL);
INSERT INTO `json_to_string_table` (`json_to_string_col`) VALUES (NULL);
INSERT INTO `longblob_table` (`longblob_col`) VALUES (NULL);
INSERT INTO `longblob_to_string_table` (`longblob_to_string_col`) VALUES (NULL);
INSERT INTO `longtext_table` (`longtext_col`) VALUES (NULL);
INSERT INTO `mediumblob_table` (`mediumblob_col`) VALUES (NULL);
INSERT INTO `mediumblob_to_string_table` (`mediumblob_to_string_col`) VALUES (NULL);
INSERT INTO `mediumint_table` (`mediumint_col`) VALUES (NULL);
INSERT INTO `mediumint_to_string_table` (`mediumint_to_string_col`) VALUES (NULL);
INSERT INTO `mediumint_unsigned_table` (`mediumint_unsigned_col`) VALUES (NULL);
INSERT INTO `mediumtext_table` (`mediumtext_col`) VALUES (NULL);
INSERT INTO `numeric_to_numeric_table` (`numeric_to_numeric_col`) VALUES (NULL);
INSERT INTO `numeric_to_string_table` (`numeric_to_string_col`) VALUES (NULL);
INSERT INTO `real_to_float64_table` (`real_to_float64_col`) VALUES (NULL);
INSERT INTO `real_to_string_table` (`real_to_string_col`) VALUES (NULL);
INSERT INTO `set_to_array_table` (`set_to_array_col`) VALUES (NULL);
INSERT INTO `smallint_table` (`smallint_col`) VALUES (NULL);
INSERT INTO `smallint_to_string_table` (`smallint_to_string_col`) VALUES (NULL);
INSERT INTO `smallint_unsigned_table` (`smallint_unsigned_col`) VALUES (NULL);
INSERT INTO `text_table` (`text_col`) VALUES (NULL);
INSERT INTO `time_table` (`time_col`) VALUES (NULL);
INSERT INTO `timestamp_table` (`timestamp_col`) VALUES (NULL);
INSERT INTO `timestamp_to_string_table` (`timestamp_to_string_col`) VALUES (NULL);
INSERT INTO `tinyblob_table` (`tinyblob_col`) VALUES (NULL);
INSERT INTO `tinyblob_to_string_table` (`tinyblob_to_string_col`) VALUES (NULL);
INSERT INTO `tinyint_table` (`tinyint_col`) VALUES (NULL);
INSERT INTO `tinyint_to_string_table` (`tinyint_to_string_col`) VALUES (NULL);
INSERT INTO `tinyint_unsigned_table` (`tinyint_unsigned_col`) VALUES (NULL);
INSERT INTO `tinytext_table` (`tinytext_col`) VALUES (NULL);
INSERT INTO `varbinary_table` (`varbinary_col`) VALUES (NULL);
INSERT INTO `varbinary_to_string_table` (`varbinary_to_string_col`) VALUES (NULL);
INSERT INTO `varchar_table` (`varchar_col`) VALUES (NULL);
INSERT INTO `year_table` (`year_col`) VALUES (NULL);
INSERT INTO set_table (set_col) VALUES (NULL);

INSERT INTO `generated_pk_column_table`(`first_name_col`, `last_name_col`) VALUES("AA", "BB");
INSERT INTO `generated_non_pk_column_table`(`id`, `first_name_col`, `last_name_col`) VALUES(1, "AA", "BB");
INSERT INTO `generated_non_pk_column_table`(`id`, `first_name_col`, `last_name_col`) VALUES(10, "AA", "BB");
INSERT INTO `non_generated_to_generated_column_table`(`first_name_col`, `last_name_col`, `generated_column_col`, `generated_column_pk_col`) VALUES("AA", "BB", "AA ", "AA ");
INSERT INTO `generated_to_non_generated_column_table`(`first_name_col`, `last_name_col`) VALUES("AA", "BB");


CREATE TABLE IF NOT EXISTS spatial_point (
    id INT AUTO_INCREMENT PRIMARY KEY,
    location POINT
);

INSERT INTO spatial_point (location) VALUES (POINT(77.5946, 12.9716));


CREATE TABLE IF NOT EXISTS spatial_linestring (
    id INT AUTO_INCREMENT PRIMARY KEY,
    path LINESTRING
);

INSERT INTO spatial_linestring (path)
VALUES (LineString(Point(77.5946, 12.9716), Point(77.6100, 12.9600)));

CREATE TABLE IF NOT EXISTS spatial_polygon (
    id INT AUTO_INCREMENT PRIMARY KEY,
    area POLYGON
);

INSERT INTO spatial_polygon (area)
VALUES (Polygon(LineString(Point(77.5946, 12.9716), Point(77.6100, 12.9600), Point(77.6000, 12.9500), Point(77.5946, 12.9716))));

CREATE TABLE IF NOT EXISTS spatial_multipoint (
    id INT AUTO_INCREMENT PRIMARY KEY,
    points MULTIPOINT
);

INSERT INTO spatial_multipoint (points) VALUES (MultiPoint(Point(77.5946, 12.9716), Point(77.6100, 12.9600)));

CREATE TABLE IF NOT EXISTS spatial_multilinestring (
    id INT AUTO_INCREMENT PRIMARY KEY,
    paths MULTILINESTRING
);

INSERT INTO spatial_multilinestring (paths)
VALUES (MultiLineString(LineString(Point(77.5946, 12.9716), Point(77.6100, 12.9600)), LineString(Point(77.6000, 12.9500), Point(77.6200, 12.9400))));

CREATE TABLE IF NOT EXISTS spatial_multipolygon (
    id INT AUTO_INCREMENT PRIMARY KEY,
    areas MULTIPOLYGON
);

INSERT INTO spatial_multipolygon (areas)
VALUES (MultiPolygon(Polygon(LineString(Point(77.5946, 12.9716), Point(77.6100, 12.9600), Point(77.6000, 12.9500), Point(77.5946, 12.9716))),
                     Polygon(LineString(Point(77.6200, 12.9400), Point(77.6300, 12.9300), Point(77.6400, 12.9450), Point(77.6200, 12.9400)))));

CREATE TABLE IF NOT EXISTS spatial_geometry (
  id INT AUTO_INCREMENT PRIMARY KEY,
  geom GEOMETRY
);

INSERT INTO spatial_geometry (geom) VALUES (POINT(77.5946, 12.9716));

CREATE TABLE IF NOT EXISTS spatial_geometrycollection (
  id INT AUTO_INCREMENT PRIMARY KEY,
  geoms GEOMETRYCOLLECTION
);

INSERT INTO spatial_geometrycollection (geoms) VALUES (MultiPoint(Point(77.5946, 12.9716), Point(77.6100, 12.9600)));


