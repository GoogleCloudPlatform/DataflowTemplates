DROP TABLE IF EXISTS "%PREFIX%_UuidTable";
CREATE TABLE "%PREFIX%_UuidTable" (
    "Id" bigint,
    "UuidCol" uuid,
    "UuidArrayCol" uuid[],
PRIMARY KEY("Id"));
