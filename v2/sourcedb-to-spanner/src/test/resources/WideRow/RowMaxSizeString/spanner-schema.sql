CREATE TABLE WideRowTable (
  id INT64 NOT NULL,
  max_string_col_to_bytes BYTES(MAX),
  max_string_col_to_str STRING(MAX)
) PRIMARY KEY (id);
