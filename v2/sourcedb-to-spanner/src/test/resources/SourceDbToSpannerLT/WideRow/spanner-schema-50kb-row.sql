CREATE TABLE IF NOT EXISTS heavy_users (
    id INT64 NOT NULL,
    uuid STRING(36),
    full_name STRING(100),
    email STRING(100),
    country STRING(100),
    payload_1 STRING(MAX),
    payload_2 STRING(MAX),
    payload_3 STRING(MAX),
    payload_4 STRING(MAX),
    payload_5 STRING(MAX)
) PRIMARY KEY (id);
