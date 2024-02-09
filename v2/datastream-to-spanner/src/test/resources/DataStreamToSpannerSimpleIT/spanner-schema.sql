CREATE TABLE AllDatatypeColumns (
  varchar_column STRING(20) NOT NULL,
  tinyint_column STRING(MAX),
  text_column BYTES(MAX),
  date_column STRING(MAX),
  smallint_column STRING(MAX),
  mediumint_column STRING(MAX),
  int_column STRING(MAX),
  bigint_column STRING(MAX),
  float_column STRING(MAX),
  double_column STRING(MAX),
  decimal_column STRING(MAX),
  datetime_column STRING(MAX),
  timestamp_column STRING(MAX),
  time_column STRING(MAX),
  year_column STRING(MAX),
  char_column BYTES(10),
  tinyblob_column STRING(MAX),
  tinytext_column BYTES(MAX),
  blob_column STRING(MAX),
  mediumblob_column STRING(MAX),
  mediumtext_column BYTES(MAX),
  longblob_column STRING(MAX),
  longtext_column BYTES(MAX),
  enum_column STRING(MAX),
  bool_column INT64,
  other_bool_column STRING(MAX),
  binary_column STRING(MAX),
  varbinary_column STRING(MAX),
  bit_column STRING(MAX),
) PRIMARY KEY(varchar_column);

CREATE TABLE AllDatatypeColumns2 (
  varchar_column STRING(20) NOT NULL,
  tinyint_column INT64,
  text_column STRING(MAX),
  date_column DATE,
  smallint_column INT64,
  mediumint_column INT64,
  int_column INT64,
  bigint_column INT64,
  float_column FLOAT64,
  double_column FLOAT64,
  decimal_column NUMERIC,
  datetime_column TIMESTAMP,
  timestamp_column TIMESTAMP,
  time_column STRING(MAX),
  year_column STRING(MAX),
  char_column STRING(10),
  tinyblob_column BYTES(MAX),
  tinytext_column STRING(MAX),
  blob_column BYTES(MAX),
  mediumblob_column BYTES(MAX),
  mediumtext_column STRING(MAX),
  longblob_column BYTES(MAX),
  longtext_column STRING(MAX),
  enum_column STRING(MAX),
  bool_column BOOL,
  binary_column BYTES(MAX),
  varbinary_column BYTES(MAX),
  bit_column BYTES(MAX),
) PRIMARY KEY(varchar_column);

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