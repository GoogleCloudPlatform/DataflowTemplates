CREATE TABLE parent (
    id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE child1 (
    id BIGINT NOT NULL,
    parent_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (id),
    KEY parent_id (parent_id),
    CONSTRAINT child1_ibfk_1 FOREIGN KEY (parent_id)
        REFERENCES parent(id) ON DELETE CASCADE
);

CREATE TABLE child2 (
    id BIGINT NOT NULL,
    child1_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (id),
    KEY child1_id (child1_id),
    CONSTRAINT child2_ibfk_1 FOREIGN KEY (child1_id)
        REFERENCES child1(id) ON DELETE CASCADE
);

CREATE TABLE child3 (
    id BIGINT NOT NULL,
    child2_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (id),
    KEY child2_id (child2_id),
    CONSTRAINT child3_ibfk_1 FOREIGN KEY (child2_id)
        REFERENCES child2(id) ON DELETE CASCADE
);

CREATE TABLE child4 (
    id BIGINT NOT NULL,
    child3_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (id),
    KEY child3_id (child3_id),
    CONSTRAINT child4_ibfk_1 FOREIGN KEY (child3_id)
        REFERENCES child3(id) ON DELETE CASCADE
);

CREATE TABLE child5 (
    id BIGINT NOT NULL,
    child4_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (id),
    KEY child4_id (child4_id),
    CONSTRAINT child5_ibfk_1 FOREIGN KEY (child4_id)
        REFERENCES child4(id) ON DELETE CASCADE
);

CREATE TABLE child6 (
    id BIGINT NOT NULL,
    child5_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (id),
    KEY child5_id (child5_id),
    CONSTRAINT child6_ibfk_1 FOREIGN KEY (child5_id)
        REFERENCES child5(id) ON DELETE CASCADE
);

CREATE TABLE child7 (
    id BIGINT NOT NULL,
    child6_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (id),
    KEY child6_id (child6_id),
    CONSTRAINT child7_ibfk_1 FOREIGN KEY (child6_id)
        REFERENCES child6(id) ON DELETE CASCADE
);

INSERT INTO parent (id, name) VALUES (1, 'Parent 1');
INSERT INTO child1 (id, parent_id, name) VALUES (101, 1, 'Child1 Row 101');
INSERT INTO child2 (id, child1_id, name) VALUES (201, 101, 'Child2 Row 201');
INSERT INTO child3 (id, child2_id, name) VALUES (301, 201, 'Child3 Row 301');
INSERT INTO child4 (id, child3_id, name) VALUES (401, 301, 'Child4 Row 401');
INSERT INTO child5 (id, child4_id, name) VALUES (501, 401, 'Child5 Row 501');
INSERT INTO child6 (id, child5_id, name) VALUES (601, 501, 'Child6 Row 601');
INSERT INTO child7 (id, child6_id, name) VALUES (701, 601, 'Child7 Row 701');
