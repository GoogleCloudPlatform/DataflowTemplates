CREATE TABLE `Users` (
    `id` int NOT NULL,     -- To: id INT64
    `name` varchar(200),   -- To: name STRING(200)
    `age` bigint,          -- To: age_spanner INT64  Column name renamed
    PRIMARY KEY (`id`)
);