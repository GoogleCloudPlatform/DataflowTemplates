CREATE TABLE vehicles (
    id INT PRIMARY KEY,
    make VARCHAR(50)
);

CREATE TABLE cars (
    doors INT,
    PRIMARY KEY (id)
) INHERITS (vehicles);

CREATE TABLE sports_cars (
    top_speed INT,
    PRIMARY KEY (id)
) INHERITS (cars);

INSERT INTO vehicles (id, make) VALUES (1, 'Generic Vehicle');
INSERT INTO cars (id, make, doors) VALUES (2, 'Honda', 4);
INSERT INTO sports_cars (id, make, doors, top_speed) VALUES (3, 'Ferrari', 2, 200);
