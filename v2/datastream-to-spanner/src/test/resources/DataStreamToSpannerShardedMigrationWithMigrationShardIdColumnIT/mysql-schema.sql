CREATE TABLE `Users` (
    `id` int NOT NULL,     -- To: id INT64
    `name` varchar(200),   -- To: name STRING(200)
    `age` bigint,          -- To: age_spanner INT64  Column name renamed
    PRIMARY KEY (`id`)
);

CREATE TABLE `Movie` (
    `id1`   int NOT NULL,    -- To: id1 INT64
    `id2`   int NOT NULL,    -- To: id2 INT64
    `name`  varchar(200),    -- To: name STRING(200)
    `actor` int,             -- To: actor INT64
    PRIMARY KEY (`id1`, `id2`)       -- Primary keys are reordered in Spanner. Order - (id2, id1, migration_shard_id)
);
