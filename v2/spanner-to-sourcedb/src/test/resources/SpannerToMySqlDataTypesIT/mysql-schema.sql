CREATE TABLE `varchar_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `varchar_col` VARCHAR(21000) CHARACTER SET utf8 DEFAULT NULL
);

CREATE TABLE `tinyint_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `tinyint_col` TINYINT DEFAULT NULL
);

CREATE TABLE `tinyint_unsigned_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `tinyint_unsigned_col` TINYINT UNSIGNED DEFAULT NULL
);

CREATE TABLE `text_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `text_col` TEXT CHARACTER SET utf8 DEFAULT NULL
);

CREATE TABLE `date_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `date_col` DATE DEFAULT NULL
);

CREATE TABLE `smallint_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `smallint_col` SMALLINT DEFAULT NULL
);

CREATE TABLE `smallint_unsigned_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `smallint_unsigned_col` SMALLINT UNSIGNED DEFAULT NULL
);

CREATE TABLE `mediumint_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `mediumint_col` MEDIUMINT DEFAULT NULL
);

CREATE TABLE `mediumint_unsigned_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `mediumint_unsigned_col` MEDIUMINT UNSIGNED DEFAULT NULL
);
CREATE TABLE `bigint_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `bigint_col` BIGINT DEFAULT NULL
);

CREATE TABLE `bigint_unsigned_table` (
     `id` INT AUTO_INCREMENT PRIMARY KEY,
     `bigint_unsigned_col` BIGINT UNSIGNED DEFAULT NULL
);

CREATE TABLE `float_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `float_col` FLOAT DEFAULT NULL
);

CREATE TABLE `double_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `double_col` DOUBLE DEFAULT NULL
);

CREATE TABLE `decimal_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `decimal_col` DECIMAL(65,30) DEFAULT NULL
);

CREATE TABLE `datetime_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `datetime_col` DATETIME DEFAULT NULL
);

CREATE TABLE `time_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `time_col` TIME DEFAULT NULL
);

CREATE TABLE `year_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `year_col` YEAR DEFAULT NULL
);

CREATE TABLE `char_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `char_col` CHAR(255) CHARACTER SET utf8 DEFAULT NULL
);

CREATE TABLE `tinyblob_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `tinyblob_col` TINYBLOB DEFAULT NULL
);

CREATE TABLE `tinytext_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `tinytext_col` TINYTEXT CHARACTER SET utf8 DEFAULT NULL
);

CREATE TABLE `blob_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `blob_col` BLOB DEFAULT NULL
);

CREATE TABLE `mediumblob_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `mediumblob_col` MEDIUMBLOB DEFAULT NULL
);

CREATE TABLE `mediumtext_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `mediumtext_col` MEDIUMTEXT CHARACTER SET utf8 DEFAULT NULL
);

CREATE TABLE `test_json_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `test_json_col` JSON DEFAULT NULL
);

CREATE TABLE `longblob_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `longblob_col` LONGBLOB DEFAULT NULL
);

CREATE TABLE `longtext_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `longtext_col` LONGTEXT CHARACTER SET utf8 DEFAULT NULL
);

CREATE TABLE `enum_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `enum_col` ENUM('1','2','3') CHARACTER SET utf8 DEFAULT NULL
);

CREATE TABLE `bool_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `bool_col` TINYINT(1) DEFAULT NULL
);

CREATE TABLE `binary_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `binary_col` BINARY(255) DEFAULT NULL
);

CREATE TABLE `varbinary_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `varbinary_col` VARBINARY(65000) DEFAULT NULL
);

CREATE TABLE `bit_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `bit_col` BIT(64) DEFAULT NULL
);

CREATE TABLE `boolean_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `boolean_col` TINYINT(1) DEFAULT NULL
);

CREATE TABLE `int_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `int_col` INT DEFAULT NULL
);

CREATE TABLE `integer_unsigned_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `integer_unsigned_col` INTEGER UNSIGNED DEFAULT NULL
);

CREATE TABLE `timestamp_table` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `timestamp_col` TIMESTAMP DEFAULT NULL
);

CREATE TABLE set_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
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
