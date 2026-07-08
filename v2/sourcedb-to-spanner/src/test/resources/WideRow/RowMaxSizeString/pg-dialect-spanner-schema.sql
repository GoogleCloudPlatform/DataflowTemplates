CREATE TABLE "WideRowTable" (
  id INT8 NOT NULL,
  max_string_col_to_bytes BYTEA,
  max_string_col_to_str VARCHAR(2621440),
  PRIMARY KEY (id)
);
