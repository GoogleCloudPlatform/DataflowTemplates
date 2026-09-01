CREATE TABLE DateData (
    id INT NOT NULL,
    timestamp_column DATETIMEOFFSET,
    datetime_column DATETIME2,
    timestamp_tz_column DATETIMEOFFSET,
    legacy_datetime_column DATETIME,
 PRIMARY KEY(id));

INSERT INTO DateData (id, timestamp_column, datetime_column, timestamp_tz_column, legacy_datetime_column) VALUES
    (1, CAST('2024-02-02 10:00:00.0' AS DATETIME2) AT TIME ZONE 'E. Australia Standard Time', '2024-02-02 10:00:00.0', '2024-02-02 10:00:00.0 +10:00', '2024-02-02 10:00:00.0'),
    (2, CAST('2024-02-02 20:00:00.0' AS DATETIME2) AT TIME ZONE 'E. Australia Standard Time', '2024-02-02 20:00:00.0', '2024-02-02 20:00:00.0 +10:00', '2024-02-02 20:00:00.0'),
    (3, CAST('2024-02-03 06:00:00.0' AS DATETIME2) AT TIME ZONE 'E. Australia Standard Time', '2024-02-03 06:00:00.0', '2024-02-03 06:00:00.0 +10:00', '2024-02-03 06:00:00.0');
