CREATE TABLE new_parent (
    id INT64 NOT NULL,
    name STRING(255) NOT NULL
) PRIMARY KEY (id);

CREATE TABLE new_child1 (
    id INT64 NOT NULL,
    parent_id INT64 NOT NULL,
    name STRING(255) NOT NULL
) PRIMARY KEY (id);

CREATE INDEX new_parent_id ON new_child1 (parent_id);

ALTER TABLE new_child1 ADD CONSTRAINT new_child1_ibfk_1 FOREIGN KEY (parent_id)
    REFERENCES new_parent (id) ON DELETE CASCADE;

CREATE TABLE new_child2 (
    id INT64 NOT NULL,
    child1_id INT64 NOT NULL,
    name STRING(255) NOT NULL
) PRIMARY KEY (id);

CREATE INDEX new_child1_id ON new_child2 (child1_id);

ALTER TABLE new_child2 ADD CONSTRAINT new_child2_ibfk_1 FOREIGN KEY (child1_id)
    REFERENCES new_child1 (id) ON DELETE CASCADE;

CREATE TABLE new_child3 (
    id INT64 NOT NULL,
    child2_id INT64 NOT NULL,
    name STRING(255) NOT NULL
) PRIMARY KEY (id);

CREATE INDEX new_child2_id ON new_child3 (child2_id);

ALTER TABLE new_child3 ADD CONSTRAINT new_child3_ibfk_1 FOREIGN KEY (child2_id)
    REFERENCES new_child2 (id) ON DELETE CASCADE;

CREATE TABLE new_child4 (
    id INT64 NOT NULL,
    child3_id INT64 NOT NULL,
    name STRING(255) NOT NULL
) PRIMARY KEY (id);

CREATE INDEX new_child3_id ON new_child4 (child3_id);

ALTER TABLE new_child4 ADD CONSTRAINT new_child4_ibfk_1 FOREIGN KEY (child3_id)
    REFERENCES new_child3 (id) ON DELETE CASCADE;

CREATE TABLE new_child5 (
    id INT64 NOT NULL,
    child4_id INT64 NOT NULL,
    name STRING(255) NOT NULL
) PRIMARY KEY (id);

CREATE INDEX new_child4_id ON new_child5 (child4_id);

ALTER TABLE new_child5 ADD CONSTRAINT new_child5_ibfk_1 FOREIGN KEY (child4_id)
    REFERENCES new_child4 (id) ON DELETE CASCADE;

CREATE TABLE new_child6 (
    id INT64 NOT NULL,
    child5_id INT64 NOT NULL,
    name STRING(255) NOT NULL
) PRIMARY KEY (id);

CREATE INDEX new_child5_id ON new_child6 (child5_id);

ALTER TABLE new_child6 ADD CONSTRAINT new_child6_ibfk_1 FOREIGN KEY (child5_id)
    REFERENCES new_child5 (id) ON DELETE CASCADE;

CREATE TABLE new_child7 (
    id INT64 NOT NULL,
    child6_id INT64 NOT NULL,
    name STRING(255) NOT NULL
) PRIMARY KEY (id);

CREATE INDEX new_child6_id ON new_child7 (child6_id);

ALTER TABLE new_child7 ADD CONSTRAINT new_child7_ibfk_1 FOREIGN KEY (child6_id)
    REFERENCES new_child6 (id) ON DELETE CASCADE;

-- New table: new_child8, referencing new_child7.
CREATE TABLE new_child8 (
    id INT64 NOT NULL,
    child7_id INT64 NOT NULL,
    name STRING(255) NOT NULL
) PRIMARY KEY (id);

CREATE INDEX new_child7_id ON new_child8 (child7_id);

ALTER TABLE new_child8 ADD CONSTRAINT new_child8_ibfk_1 FOREIGN KEY (child7_id)
    REFERENCES new_child7 (id) ON DELETE CASCADE;
