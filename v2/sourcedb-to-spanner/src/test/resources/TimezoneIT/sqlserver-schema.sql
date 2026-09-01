CREATE TABLE DateData (
    id INT NOT NULL,
    timestamp_column DATETIMEOFFSET,
    datetime_column DATETIME2,
 PRIMARY KEY(id));

INSERT INTO DateData VALUES
    (1, '2024-02-02 10:00:00.0 +10:00', '2024-02-02 10:00:00.0'),
    (2, '2024-02-02 20:00:00.0 +10:00', '2024-02-02 20:00:00.0'),
    (3, '2024-02-03 06:00:00.0 +10:00', '2024-02-03 06:00:00.0');
