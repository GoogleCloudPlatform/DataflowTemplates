CREATE TABLE IF NOT EXISTS customers (
    id INT64 NOT NULL,
    full_name STRING(125),
    first_name STRING(25),
    last_name STRING(25)
) PRIMARY KEY(id);