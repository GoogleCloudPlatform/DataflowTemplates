CREATE TABLE parent_table (
  id INT PRIMARY KEY,
  name VARCHAR(50)
);

CREATE TABLE child_table (
  age INT,
  PRIMARY KEY (id)
) INHERITS (parent_table);

CREATE TABLE grandchild_table (
  city VARCHAR(50),
  PRIMARY KEY (id)
) INHERITS (child_table);
