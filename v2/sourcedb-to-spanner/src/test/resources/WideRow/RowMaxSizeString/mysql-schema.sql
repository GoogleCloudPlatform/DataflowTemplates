CREATE TABLE WideRowTable (
  id INT PRIMARY KEY,
  max_string_col_to_bytes LONGTEXT,
  max_string_col_to_str MEDIUMTEXT
);

INSERT INTO WideRowTable (id, max_string_col_to_bytes, max_string_col_to_str) VALUES (1, REPEAT('a', 10485760), REPEAT('a', 2621440));
INSERT INTO WideRowTable (id, max_string_col_to_bytes, max_string_col_to_str) VALUES (2, REPEAT('b', 20971520), NULL);
INSERT INTO WideRowTable (id, max_string_col_to_bytes, max_string_col_to_str) VALUES (3, NULL, REPEAT('b', 2883584));
