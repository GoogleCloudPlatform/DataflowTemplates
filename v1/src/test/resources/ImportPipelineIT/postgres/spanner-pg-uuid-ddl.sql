DROP TABLE IF EXISTS UuidTable;
CREATE TABLE UuidTable (
                           Key uuid PRIMARY KEY,
                           Val1 uuid,
                           Val2 INT,
                           Val3 uuid[]
);
