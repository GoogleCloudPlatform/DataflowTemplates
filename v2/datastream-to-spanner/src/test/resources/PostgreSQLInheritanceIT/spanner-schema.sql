CREATE TABLE parent_table (
  id INT64 NOT NULL,
  name STRING(50)
) PRIMARY KEY(id);

CREATE TABLE child_table (
  id INT64 NOT NULL,
  name STRING(50),
  age INT64
) PRIMARY KEY(id);

CREATE TABLE grandchild_table (
  id INT64 NOT NULL,
  name STRING(50),
  age INT64,
  city STRING(50)
) PRIMARY KEY(id);
