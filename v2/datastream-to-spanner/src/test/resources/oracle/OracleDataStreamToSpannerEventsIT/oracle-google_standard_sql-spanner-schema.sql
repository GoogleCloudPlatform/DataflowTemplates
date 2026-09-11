CREATE TABLE Movie (
  id INT64 NOT NULL,
  name STRING(200),
  actor NUMERIC,
  startTime TIMESTAMP,
) PRIMARY KEY(id);

CREATE TABLE Users (
    id INT64 NOT NULL,
    name STRING(200),
    age INT64,
    subscribed BOOL,
    plan STRING(1),
    startDate TIMESTAMP,
) PRIMARY KEY (id);

CREATE TABLE Authors (
    author_id INT64 NOT NULL,
    name STRING(200),
) PRIMARY KEY (author_id);

CREATE TABLE Books (
    id INT64 NOT NULL,
    title STRING(200),
    author_id INT64,
) PRIMARY KEY (id);

CREATE INDEX author_id_6 ON Books (author_id);

ALTER TABLE Books ADD CONSTRAINT Books_ibfk_1 FOREIGN KEY (author_id) REFERENCES Authors (author_id);

CREATE TABLE Articles (
    id INT64 NOT NULL,
    name STRING(200),
    published_date TIMESTAMP,
    author_id INT64 NOT NULL,
) PRIMARY KEY (author_id, id),
INTERLEAVE IN PARENT Authors;

CREATE INDEX author_id ON Articles (author_id);
