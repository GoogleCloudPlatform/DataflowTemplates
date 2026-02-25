DROP TABLE IF EXISTS `%PREFIX%_UuidTable`;
CREATE TABLE `%PREFIX%_UuidTable` (
                Id INT64 NOT NULL,
                UuidCol UUID,
                UuidArrayCol ARRAY<UUID>,
) PRIMARY KEY(Id);
