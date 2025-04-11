DROP TABLE IF EXISTS UuidTable;
CREATE TABLE UuidTable (
                           Key UUID,
                           Val1 UUID,
                           Val2 INT64,
                           Val3 ARRAY<UUID>,
) PRIMARY KEY(Key);
