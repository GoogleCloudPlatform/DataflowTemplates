-- Spanner schema for testing very large string migration
CREATE TABLE WideRowTable (
  id INT64 NOT NULL,
  max_string_col STRING(MAX),
) PRIMARY KEY (id);
