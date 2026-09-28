CREATE TABLE IF NOT EXISTS `generated_pk_column_table` (
	`first_name_col` STRING(50),
	`last_name_col` STRING(50) DEFAULT(NULL),
	`generated_column_col` STRING(100) AS (concat(`first_name_col`,' ')) STORED,	
) PRIMARY KEY (`generated_column_col`);

CREATE TABLE IF NOT EXISTS `generated_non_pk_column_table` ( 
	`first_name_col` STRING(50),
	`last_name_col` STRING(50) DEFAULT(NULL),
	`generated_column_col` STRING(100) AS (concat(`first_name_col`,' ')) STORED,
  `id` INT64 not null,
) PRIMARY KEY (`id`);

CREATE TABLE IF NOT EXISTS `non_generated_to_generated_column_table` ( 
	`first_name_col` STRING(50),
	`last_name_col` STRING(50) DEFAULT(NULL),
	`generated_column_col` STRING(100) AS (concat(`first_name_col`,' ')) STORED,
	`generated_column_pk_col` STRING(100) AS (concat(`first_name_col`,' ')) STORED,
) PRIMARY KEY (`generated_column_pk_col`);

CREATE TABLE IF NOT EXISTS `generated_to_non_generated_column_table` ( 
	`first_name_col` STRING(50),
	`last_name_col` STRING(50) DEFAULT(NULL),
	`generated_column_col` STRING(100) DEFAULT(NULL),
	`generated_column_pk_col` STRING(100) DEFAULT(NULL),
) PRIMARY KEY (`generated_column_pk_col`);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d',
  allow_txn_exclusion = true
);
