CREATE TABLE IF NOT EXISTS WideRowTable(
id INT64 NOT NULL,
max_bytes_col_1 BYTES(MAX),
max_bytes_col_2 BYTES(MAX),
max_bytes_col_3 BYTES(MAX),
max_bytes_col_4 BYTES(MAX),
max_bytes_col_5 BYTES(MAX),
max_bytes_col_6 BYTES(MAX)
) PRIMARY KEY (id)