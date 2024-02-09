CREATE TABLE `Movie` (
    `id` int NOT NULL,       -- To: id INT64
    `name` varchar(200),     -- To: name STRING(200)
    `actor` decimal(65,30),  -- To: actor NUMERIC
    `startTime` timestamp,   -- To: startTime TIMESTAMP
    PRIMARY KEY (`id`)
);

CREATE TABLE `Users` (
    `id` int NOT NULL,     -- To: id INT64
    `name` varchar(200),   -- To: name STRING(200)
    `age` bigint,          -- To: age_spanner INT64  Column name renamed
    `subscribed` bit(1),   -- To: subscribed BOOL
    `plan` char(1),        -- To: plan STRING(1)
    `startDate` date,      -- To: startDate DATE
    PRIMARY KEY (`id`)
);

CREATE TABLE `Category` (
  `category_id` tinyint NOT NULL, -- To: category_id INT64
  `name` varchar(25),             -- To: full_name STRING(25) Column name renamed
  `last_update` timestamp,        -- To: Column dropped in spanner
  PRIMARY KEY (`category_id`)
);

-- Database postgres_test in shreya-mysql
CREATE TABLE `AllDatatypeColumns` (
  `varchar_column` varchar(20) NOT NULL, -- To: varchar_column BYTES(20)
  `tinyint_column` tinyint,              -- To: tinyint_column STRING(MAX)
  `text_column` text,                    -- To: text_column BYTES(MAX)
  `date_column` date,                    -- To: date_column STRING(MAX)
  `smallint_column` smallint,            -- To: smallint_column STRING(MAX)
  `mediumint_column` mediumint,          -- To: mediumint_column STRING(MAX)
  `int_column` int,                      -- To: int_column STRING(MAX)
  `bigint_column` bigint,                -- To: bigint_column STRING(MAX)
  `float_column` float(10,2),            -- To: float_column STRING(MAX)
  `double_column` double,                -- To: double_column STRING(MAX)
  `decimal_column` decimal(10,2),        -- To: decimal_column STRING(MAX)
  `datetime_column` datetime,            -- To: datetime_column STRING(MAX)
  `timestamp_column` timestamp,          -- To: timestamp_column STRING(MAX)
  `time_column` time,                    -- To: time_column STRING(MAX)
  `year_column` year,                    -- To: year_column STRING(MAX)
  `char_column` char(10),                -- To: char_column BYTES(10)
  `tinyblob_column` tinyblob,            -- To: tinyblob_column STRING(MAX)
  `tinytext_column` tinytext,            -- To: tinytext_column BYTES(MAX)
  `blob_column` blob,                    -- To: blob_column STRING(MAX)
  `mediumblob_column` mediumblob,        -- To: mediumblob_column STRING(MAX)
  `mediumtext_column` mediumtext,        -- To: mediumtext_column BYTES(MAX)
  `longblob_column` longblob,            -- To: longblob_column STRING(MAX)
  `longtext_column` longtext,            -- To: longtext_column BYTES(MAX)
  `enum_column` enum('1','2','3'),       -- To: enum_column STRING(MAX)
  `bool_column` tinyint(1),              -- To: bool_column INT64
  `other_bool_column` tinyint(1),        -- To: other_bool_column STRING(MAX)
  `binary_column` binary(20),            -- To: binary_column STRING(MAX)
  `varbinary_column` varbinary(20),      -- To: varbinary_column STRING(MAX)
  `bit_column` bit(7),                   -- To: bit_column STRING(MAX)
  PRIMARY KEY (`varchar_column`)
);

CREATE TABLE `AllDatatypeColumns2` (
 `varchar_column` varchar(20) NOT NULL, -- To: varchar_column BYTES(20)
 `tinyint_column` tinyint,              -- To: tinyint_column INT64
 `text_column` text,                    -- To: text_column STRING(MAX)
 `date_column` date,                    -- To: date_column DATE
 `smallint_column` smallint,            -- To: smallint_column INT64
 `mediumint_column` mediumint,          -- To: mediumint_column INT64
 `int_column` int,                      -- To: int_column INT64
 `bigint_column` bigint,                -- To: bigint_column INT64
 `float_column` float(10,2),            -- To: float_column FLOAT64
 `double_column` double,                -- To: double_column FLOAT64
 `decimal_column` decimal(10,2),        -- To: decimal_column NUMERIC
 `datetime_column` datetime,            -- To: datetime_column TIMESTAMP
 `timestamp_column` timestamp,          -- To: timestamp_column TIMESTAMP
 `time_column` time,                    -- To: time_column STRING(MAX)
 `year_column` year,                    -- To: year_column STRING(MAX)
 `char_column` char(10),                -- To: char_column STRING(10)
 `tinyblob_column` tinyblob,            -- To: tinyblob_column BYTES(MAX)
 `tinytext_column` tinytext,            -- To: tinytext_column STRING(MAX)
 `blob_column` blob,                    -- To: blob_column BYTES(MAX)
 `mediumblob_column` mediumblob,        -- To: mediumblob_column BYTES(MAX)
 `mediumtext_column` mediumtext,        -- To: mediumtext_column STRING(MAX)
 `longblob_column` longblob,            -- To: longblob_column BYTES(MAX)
 `longtext_column` longtext,            -- To: longtext_column STRING(MAX)
 `enum_column` enum('1','2','3'),       -- To: enum_column STRING(MAX)
 `bool_column` tinyint(1),              -- To: bool_column BOOL
 `binary_column` binary(20),            -- To: binary_column BYTES(MAX)
 `varbinary_column` varbinary(20),      -- To: varbinary_column BYTES(MAX)
 `bit_column` bit(7),                   -- To: bit_column BYTES(MAX)
 PRIMARY KEY (`varchar_column`)
);

CREATE TABLE `Authors` (
    `author_id` int NOT NULL,
    `name` varchar(200),
    PRIMARY KEY (`author_id`)
);

CREATE TABLE `Articles` (
    `id` int NOT NULL,
    `name` varchar(200),
    `published_date` date,
    `author_id` int,          -- To: author_id INT64
    PRIMARY KEY (`id`)        -- To: Changed PK to PRIMARY_KEY(author_id, id) in Spanner and Interleaved in Authors
);

ALTER TABLE `Articles` add FOREIGN KEY (`author_id`) references Authors(`author_id`);  -- To: Foreign key converted to Interleaved in Spanner

CREATE INDEX author_id ON Articles (author_id);  -- To: Index retained in Spanner

CREATE TABLE `Books` (
    `id` int NOT NULL,
    `title` varchar(200),
    `author_id` int,
    PRIMARY KEY (`id`)
);

ALTER TABLE `Books` add FOREIGN KEY (`author_id`) references Authors(`author_id`);  -- To: Foreign Key retained as is in Spanner

CREATE INDEX author_id_6 ON Books (author_id);  -- To: Index retained in Spanner
