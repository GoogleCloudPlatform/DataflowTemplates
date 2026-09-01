CREATE TABLE Users (
  id BIGINT PRIMARY KEY,
  full_name VARCHAR(255),
  location VARCHAR(255)
);

CREATE TABLE Users2 (
  id BIGINT PRIMARY KEY,
  name VARCHAR(255)
);

CREATE TABLE AllDatatypes (
  id BIGINT PRIMARY KEY,
  tinyint_col TINYINT,
  smallint_col SMALLINT,
  int_col INT,
  bigint_col BIGINT,
  bit_col BIT,
  numeric_col NUMERIC(18, 2),
  float_col FLOAT,
  varchar_col VARCHAR(255),
  date_col DATE,
  timestamp_col DATETIME2
);
