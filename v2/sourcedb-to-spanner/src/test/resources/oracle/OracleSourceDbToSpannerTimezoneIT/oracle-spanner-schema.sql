ALTER DATABASE db SET OPTIONS (default_time_zone = 'Australia/Brisbane');

CREATE TABLE DateData (
    id INT64 NOT NULL,
    timestamp_column TIMESTAMP,
    datetime_column TIMESTAMP,
    timestamp_tz_column TIMESTAMP,
    date_column TIMESTAMP,
) PRIMARY KEY(id);
