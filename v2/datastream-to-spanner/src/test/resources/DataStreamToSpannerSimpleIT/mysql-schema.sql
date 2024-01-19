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
