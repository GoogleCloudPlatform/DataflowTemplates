CREATE TABLE Category (
  category_id INT64 NOT NULL,
  full_name STRING(25),
) PRIMARY KEY(category_id);

CREATE TABLE Movie (
  id INT64 NOT NULL,
  name STRING(200),
  actor NUMERIC,
  startTime TIMESTAMP,
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS Users (
    id INT64 NOT NULL,
    name STRING(200),
    age_spanner INT64,
    subscribed BOOL,
    plan STRING(1),
    startDate DATE
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS Authors (
    author_id INT64 NOT NULL,
    name STRING(200),
) PRIMARY KEY (author_id);

CREATE TABLE IF NOT EXISTS Books (
    id INT64 NOT NULL,
    title STRING(200),
    author_id INT64,
) PRIMARY KEY (id);

CREATE INDEX author_id_6 ON Books (author_id);

ALTER TABLE Books ADD CONSTRAINT Books_ibfk_1 FOREIGN KEY (author_id) REFERENCES Authors (author_id);

CREATE TABLE IF NOT EXISTS Articles (
    id INT64 NOT NULL,
    name STRING(200),
    published_date DATE,
    author_id INT64 NOT NULL,
) PRIMARY KEY (author_id, id),
INTERLEAVE IN PARENT Authors;

CREATE INDEX author_id ON Articles (author_id);