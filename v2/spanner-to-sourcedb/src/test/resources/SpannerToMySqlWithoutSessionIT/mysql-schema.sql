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
