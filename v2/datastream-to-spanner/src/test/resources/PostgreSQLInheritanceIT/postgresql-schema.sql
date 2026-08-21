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

INSERT INTO parent_table (id, name) VALUES (1, 'Parent Row 1');
INSERT INTO child_table (id, name, age) VALUES (2, 'Child Row 1', 10);
INSERT INTO grandchild_table (id, name, age, city) VALUES (3, 'Grandchild Row 1', 5, 'New York');
