CREATE TABLE DateData (
    id INT NOT NULL,
    timestamp_column TIMESTAMP,
    datetime_column DATETIME,
 PRIMARY KEY(id));

SET time_zone = 'Australia/Brisbane';

INSERT INTO DateData VALUES
    (1, '2024-02-02 10:00:00.0', '2024-02-02 10:00:00.0'),
    (2, '2024-02-02 20:00:00.0', '2024-02-02 20:00:00.0'),
    (3, '2024-02-03 06:00:00.0', '2024-02-03 06:00:00.0');
