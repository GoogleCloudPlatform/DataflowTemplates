CREATE TABLE `Authors` (
    `author_id` int NOT NULL, -- To: category_id INT64
    `name` varchar(25),             -- To: full_name STRING(25) Column name renamed
    `last_update` timestamp,        -- To: Column dropped in spanner
    PRIMARY KEY (`author_id`)
);

CREATE TABLE `Books` (
    `id` int NOT NULL,
    `title` varchar(200),
    `author_id` int,
    FOREIGN KEY (author_id) REFERENCES Author(author_id)
);

CREATE TABLE `Genre` (
    `genre_id` int not NULL,
    `name` varchar(200),
    PRIMARY KEY (`genre_id`)
);