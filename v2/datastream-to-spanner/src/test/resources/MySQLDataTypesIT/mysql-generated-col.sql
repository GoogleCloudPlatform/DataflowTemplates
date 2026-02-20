DELETE FROM `generated_pk_column_table` WHERE `first_name_col` = "AA";
DELETE FROM `generated_non_pk_column_table` WHERE `id` = 1;
DELETE FROM `non_generated_to_generated_column_table` WHERE `first_name_col` = "AA";
DELETE FROM `generated_to_non_generated_column_table` WHERE `first_name_col` = "AA";

INSERT INTO `generated_pk_column_table`(`first_name_col`, `last_name_col`) VALUES("BB", "CC");
INSERT INTO `generated_non_pk_column_table`(`id`, `first_name_col`, `last_name_col`) VALUES(2, "BB", "CC");
INSERT INTO `non_generated_to_generated_column_table`(`first_name_col`, `last_name_col`, `generated_column_col`, `generated_column_pk_col`) VALUES("BB", "CC", "BB ", "BB ");
INSERT INTO `generated_to_non_generated_column_table`(`first_name_col`, `last_name_col`) VALUES("BB", "CC");

UPDATE `generated_pk_column_table` SET `first_name_col` = "CC" WHERE `first_name_col` = "BB";
UPDATE `generated_non_pk_column_table` SET `first_name_col` = "CC" WHERE `id` = 2;
UPDATE `generated_non_pk_column_table` SET `id` = 11 WHERE `id` = 10;
UPDATE `non_generated_to_generated_column_table` SET `first_name_col` = "CC" WHERE `first_name_col` = "BB";
UPDATE `generated_to_non_generated_column_table` SET `first_name_col` = "CC" WHERE `first_name_col` = "BB";

INSERT INTO `generated_non_pk_column_table`(`id`, `first_name_col`, `last_name_col`) VALUES(3, "DD", "EE");
