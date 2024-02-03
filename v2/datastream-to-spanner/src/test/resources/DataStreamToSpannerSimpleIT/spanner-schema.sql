CREATE TABLE IF NOT EXISTS Movie (
    id INT64 NOT NULL,
    name STRING(200),
    actor NUMERIC,
    startTime TIMESTAMP
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS Users (
    id INT64 NOT NULL,
    name STRING(200),
    age_spanner INT64,
    subscribed BOOL,
    plan STRING(1),
    startDate DATE
) PRIMARY KEY (id);
