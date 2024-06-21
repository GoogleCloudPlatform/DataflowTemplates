CREATE TABLE `AllDatatypeColumns` (
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
    `other_bool_column` tinyint(1),        -- To: other_bool_column BOOL
    `binary_column` binary(20),            -- To: binary_column BYTES(MAX)
    `varbinary_column` varbinary(20),      -- To: varbinary_column BYTES(MAX)
    `bit_column` bit(7),                   -- To: bit_column BYTES(MAX)
    PRIMARY KEY (`varchar_column`)
);
