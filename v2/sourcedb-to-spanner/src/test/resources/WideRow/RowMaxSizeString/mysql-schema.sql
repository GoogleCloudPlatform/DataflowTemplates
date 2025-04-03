CREATE TABLE WideRowTable (
  id INT PRIMARY KEY,
  max_string_col MEDIUMTEXT
);

INSERT INTO WideRowTable (id, max_string_col) VALUES (1, REPEAT('a', 10485760));
INSERT INTO WideRowTable (id, max_string_col) VALUES (2, REPEAT('b', 14680064));
