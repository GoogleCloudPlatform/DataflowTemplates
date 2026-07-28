CREATE TABLE t_bigint (
  id INT64 NOT NULL,
  col INT64,
) PRIMARY KEY(id);

CREATE TABLE t_varchar (
  id INT64 NOT NULL,
  col STRING(255),
) PRIMARY KEY(id);

CREATE TABLE t_decimal (
  id INT64 NOT NULL,
  col NUMERIC,
) PRIMARY KEY(id);
