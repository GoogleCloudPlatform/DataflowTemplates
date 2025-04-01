CREATE TABLE WideRowTable (
  id INT PRIMARY KEY,
  max_string_col MEDIUMTEXT
);

INSERT INTO WideRowTable (id, max_string_col) VALUES (1, REPEAT('a', 1000000));
INSERT INTO WideRowTable (id, max_string_col) VALUES (2, REPEAT('b', 11000000));
