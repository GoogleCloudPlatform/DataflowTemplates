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
