CREATE TABLE parent_table (
  id BIGINT PRIMARY KEY,
  name VARCHAR(50)
);

CREATE TABLE child_table (
  id BIGINT PRIMARY KEY,
  name VARCHAR(50),
  age BIGINT
);

CREATE TABLE grandchild_table (
  id BIGINT PRIMARY KEY,
  name VARCHAR(50),
  age BIGINT,
  city VARCHAR(50)
);
