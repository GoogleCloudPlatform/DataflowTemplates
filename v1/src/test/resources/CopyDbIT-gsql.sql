-- Tables with interleaving
CREATE TABLE `Users` (
	`first_name`                            STRING(MAX),
	`last_name`                             STRING(5),
	`age`                                   INT64,
) PRIMARY KEY (`first_name` ASC, `last_name` DESC);

-- All Datatypes
CREATE TABLE `AllTYPES` (
	`first_name`                            STRING(MAX),
	`last_name`                             STRING(5),
	`id`                                    INT64 NOT NULL,
	`bool_field`                            BOOL,
	`int64_field`                           INT64,
	`float32_field`                         FLOAT32,
	`float64_field`                         FLOAT64,
	`string_field`                          STRING(MAX),
	`bytes_field`                           BYTES(MAX),
	`timestamp_field`                       TIMESTAMP,
	`date_field`                            DATE,
	`arr_bool_field`                        ARRAY<BOOL>,
	`arr_int64_field`                       ARRAY<INT64>,
	`arr_float32_field`                     ARRAY<FLOAT32>,
	`arr_float64_field`                     ARRAY<FLOAT64>,
	`arr_string_field`                      ARRAY<STRING(MAX)>,
	`arr_bytes_field`                       ARRAY<BYTES(MAX)>,
	`arr_timestamp_field`                   ARRAY<TIMESTAMP>,
	`arr_date_field`                        ARRAY<DATE>,
) PRIMARY KEY (`first_name` ASC, `last_name` DESC, `id` ASC),
INTERLEAVE IN PARENT `Users` ON DELETE CASCADE;

-- Foreign Keys
CREATE TABLE `Ref` (
	`id1`                                   INT64,
	`id2`                                   INT64,
) PRIMARY KEY (`id1` ASC, `id2` ASC);
CREATE TABLE `Child` (
	`id1`                                   INT64,
	`id2`                                   INT64,
	`id3`                                   INT64,
) PRIMARY KEY (`id1` ASC, `id2` ASC, `id3` ASC),
INTERLEAVE IN PARENT `Ref`;

ALTER TABLE `Child` ADD CONSTRAINT `fk1` FOREIGN KEY (`id1`) REFERENCES `Ref` (`id1`);
ALTER TABLE `Child` ADD CONSTRAINT `fk2` FOREIGN KEY (`id2`) REFERENCES `Ref` (`id2`);
ALTER TABLE `Child` ADD CONSTRAINT `fk3` FOREIGN KEY (`id2`) REFERENCES `Ref` (`id2`);
ALTER TABLE `Child` ADD CONSTRAINT `fk4` FOREIGN KEY (`id2`, `id1`) REFERENCES `Ref` (`id2`, `id1`);
ALTER TABLE `Child` ADD CONSTRAINT `fk5` FOREIGN KEY (`id2`) REFERENCES `Ref` (`id2`) NOT ENFORCED;
ALTER TABLE `Child` ADD CONSTRAINT `fk6` FOREIGN KEY (`id2`) REFERENCES `Ref` (`id2`) ENFORCED;
