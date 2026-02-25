CREATE TABLE IF NOT EXISTS
  my_table ( id INT64 NOT NULL,
    val INT64,
    )
PRIMARY KEY
  (id);

CREATE TABLE IF NOT EXISTS
  alltypes ( bool_field BOOL NOT NULL,
    int64_field INT64 NOT NULL,
    float64_field FLOAT64 NOT NULL,
    string_field STRING(MAX) NOT NULL,
    bytes_field BYTES(MAX) NOT NULL,
    timestamp_field TIMESTAMP NOT NULL,
    date_field DATE NOT NULL,
    numeric_field NUMERIC NOT NULL,
    val INT64,
    )
PRIMARY KEY
  (bool_field,
    int64_field,
    float64_field,
    string_field,
    bytes_field,
    timestamp_field,
    date_field,
    numeric_field);