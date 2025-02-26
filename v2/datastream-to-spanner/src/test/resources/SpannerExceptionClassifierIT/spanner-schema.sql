CREATE TABLE Authors (
                         author_id INT64 NOT NULL,
                         name STRING(200),
                         CONSTRAINT check_author_id CHECK(author_id < 200),
) PRIMARY KEY(author_id);

CREATE UNIQUE INDEX idx_authors_name ON Authors(name);

CREATE TABLE Books (
                       id INT64 NOT NULL,
                       title STRING(200) NOT NULL,
                       author_id INT64 NOT NULL,
                       titleLowerStored STRING(MAX) AS (LOWER(title)) STORED,
) PRIMARY KEY(author_id, id),
  INTERLEAVE IN PARENT Authors ON DELETE NO ACTION;

CREATE TABLE ForeignKeyParent (
                                  id INT64,
                                  name STRING(200),
) PRIMARY KEY(id);

CREATE TABLE ForeignKeyChild (
                     id INT64,
                     parent_id INT64,
                     parent_name STRING(200),
) PRIMARY KEY(id);

ALTER TABLE ForeignKeyChild ADD CONSTRAINT fk_constraint1 FOREIGN KEY(parent_id) REFERENCES ForeignKeyParent(id);
ALTER TABLE ForeignKeyChild ADD CONSTRAINT fk_constraint2 FOREIGN KEY(parent_name) REFERENCES ForeignKeyParent(name);

CREATE TABLE GenPK (
                       id1 INT64,
                       id2 INT64 AS (part1 + 25) STORED,
                       part1 INT64,
                       name STRING(200),
) PRIMARY KEY(id1, id2);

CREATE TABLE MultiKeyTable (
    id1 INT64,
    id2 INT64,
    id3 INT64,
    name STRING(200)
) PRIMARY KEY(id1, id2, id3);
