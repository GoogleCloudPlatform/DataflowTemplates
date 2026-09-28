ALTER DATABASE db SET OPTIONS (default_time_zone = 'Australia/Brisbane');

CREATE TABLE IF NOT EXISTS DateData (
    id INT64 NOT NULL,
    timestamp_column TIMESTAMP,
    datetime_column TIMESTAMP,
) PRIMARY KEY(id);
