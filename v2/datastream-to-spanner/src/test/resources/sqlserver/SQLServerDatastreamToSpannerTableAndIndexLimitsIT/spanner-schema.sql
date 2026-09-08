CREATE TABLE `LargeKey` (
  `pk_col1` STRING(255) NOT NULL,
  `pk_col2` STRING(255) NOT NULL,
  `pk_col3` STRING(255) NOT NULL,
  `col1` STRING(255),
  `col2` STRING(255),
  `col3` STRING(255),
  `value_col` STRING(MAX)
) PRIMARY KEY (`pk_col1`, `pk_col2`, `pk_col3`);

CREATE INDEX `large_index` ON `LargeKey` (`col1`, `col2`, `col3`);

CREATE TABLE `LargeCell` (
  `id` INT64 NOT NULL,
  `max_string_col_to_bytes` BYTES(MAX),
  `max_string_col_to_str` STRING(MAX)
) PRIMARY KEY (`id`);

CREATE TABLE `WideRow` (
  `id` INT64 NOT NULL,
  `col1` STRING(MAX),
  `col2` STRING(MAX),
  `col3` STRING(MAX)
) PRIMARY KEY (`id`);
