CREATE TABLE Users (
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR(200),
    age INT,
    subscribed BIT,
    [plan] CHAR(1),
    startDate DATE
);

CREATE TABLE Movie (
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR(200),
    actor NUMERIC(38,9),
    startTime DATETIMEOFFSET
);

CREATE TABLE Authors (
    author_id INT NOT NULL PRIMARY KEY,
    name VARCHAR(200)
);

CREATE TABLE Books (
    id INT NOT NULL PRIMARY KEY,
    title VARCHAR(200),
    author_id INT
);

CREATE TABLE Articles (
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR(200),
    published_date DATE,
    author_id INT NOT NULL
);
