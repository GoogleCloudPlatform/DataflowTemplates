DROP TABLE IF EXISTS users;

CREATE TABLE IF NOT EXISTS users (
    id INT64 NOT NULL,
    full_name STRING(25),
    `from` STRING(25)
) PRIMARY KEY(id);

DROP TABLE IF EXISTS users2;

CREATE TABLE IF NOT EXISTS users2 (
    id INT64 NOT NULL,
    full_name STRING(25),
    ) PRIMARY KEY(id);

DROP TABLE IF EXISTS alldatatypetransformation;

CREATE TABLE IF NOT EXISTS alldatatypetransformation (
    varchar_column STRING(20) NOT NULL,
    tinyint_column STRING(MAX),
    text_column STRING(MAX),
    date_column STRING(MAX),
    smallint_column STRING(MAX),
    mediumint_column STRING(MAX),
    int_column STRING(MAX),
    bigint_column STRING(MAX),
    float_column STRING(MAX),
    double_column STRING(MAX),
    decimal_column STRING(MAX),
    datetime_column STRING(MAX),
    timestamp_column STRING(MAX),
    time_column STRING(MAX),
    year_column STRING(MAX),
    char_column STRING(10),
    tinytext_column STRING(MAX),
    mediumtext_column STRING(MAX),
    longtext_column STRING(MAX),
    enum_column STRING(MAX),
    bool_column STRING(MAX),
    other_bool_column STRING(MAX),
    list_text_column JSON,
    list_int_column JSON,
    frozen_list_bigint_column JSON,
    set_text_column JSON,
    set_date_column JSON,
    frozen_set_bool_column JSON,
    map_text_to_int_column JSON,
    map_date_to_text_column JSON,
    frozen_map_int_to_bool_column JSON,
    map_text_to_list_column JSON,
    map_text_to_set_column JSON,
    set_of_maps_column JSON,
    list_of_sets_column JSON,
    frozen_map_text_to_list_column JSON,
    frozen_map_text_to_set_column JSON,
    frozen_set_of_maps_column JSON,
    frozen_list_of_sets_column JSON,
    varint_column STRING(MAX)
) PRIMARY KEY(varchar_column);

DROP TABLE IF EXISTS alldatatypecolumns;

CREATE TABLE IF NOT EXISTS alldatatypecolumns (
    varchar_column STRING(20) NOT NULL,
    tinyint_column INT64,
    text_column STRING(MAX),
    date_column DATE,
    smallint_column INT64,
    mediumint_column INT64,
    int_column INT64,
    bigint_column INT64,
    float_column FLOAT64,
    double_column FLOAT64,
    decimal_column NUMERIC,
    datetime_column TIMESTAMP,
    timestamp_column TIMESTAMP,
    time_column STRING(MAX),
    year_column STRING(MAX),
    char_column STRING(10),
    tinytext_column STRING(MAX),
    mediumtext_column STRING(MAX),
    longtext_column STRING(MAX),
    enum_column STRING(MAX),
    bool_column BOOL,
    other_bool_column BOOL,
    bytes_column BYTES(MAX),
    list_text_column JSON,
    list_int_column JSON,
    frozen_list_bigint_column JSON,
    set_text_column JSON,
    set_date_column JSON,
    frozen_set_bool_column JSON,
    map_text_to_int_column JSON,
    map_date_to_text_column JSON,
    frozen_map_int_to_bool_column JSON,
    map_text_to_list_column JSON,
    map_text_to_set_column JSON,
    set_of_maps_column JSON,
    list_of_sets_column JSON,
    frozen_map_text_to_list_column JSON,
    frozen_map_text_to_set_column JSON,
    frozen_set_of_maps_column JSON,
    frozen_list_of_sets_column JSON,
    varint_column STRING(MAX),
    inet_column STRING(MAX)
) PRIMARY KEY(varchar_column);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);