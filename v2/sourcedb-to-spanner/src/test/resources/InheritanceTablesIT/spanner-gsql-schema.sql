CREATE TABLE vehicles (
    id INT64,
    make STRING(MAX)
) PRIMARY KEY(id);

CREATE TABLE cars (
    id INT64,
    make STRING(MAX),
    doors INT64
) PRIMARY KEY(id);

CREATE TABLE sports_cars (
    id INT64,
    make STRING(MAX),
    doors INT64,
    top_speed INT64
) PRIMARY KEY(id);
