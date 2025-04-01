-- MySQL schema for testing very large string migration
CREATE TABLE WideRowTable (
  id INT PRIMARY KEY,
  max_string_col MEDIUMTEXT
);

-- Insert a row with normal size data that should succeed
INSERT INTO WideRowTable (id, max_string_col) VALUES (1, REPEAT('a', 1000000));

-- Insert a row with string exceeding 10MB which should cause failure when migrating to Spanner
-- 11MB string (exceeds Spanner's 10MB limit)
INSERT INTO WideRowTable (id, max_string_col) VALUES (2, REPEAT('b', 11000000));
