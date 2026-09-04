CREATE TABLE t10 (
    id BIGINT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE t9 (
    id BIGINT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_t9_t10 FOREIGN KEY (id) REFERENCES t10 (id)
);

CREATE TABLE t8 (
    id BIGINT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_t8_t9 FOREIGN KEY (id) REFERENCES t9 (id)
);

CREATE TABLE t7 (
    id BIGINT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_t7_t8 FOREIGN KEY (id) REFERENCES t8 (id)
);

CREATE TABLE t6 (
    id BIGINT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_t6_t7 FOREIGN KEY (id) REFERENCES t7 (id)
);

CREATE TABLE t5 (
    id BIGINT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_t5_t6 FOREIGN KEY (id) REFERENCES t6 (id)
);

CREATE TABLE t4 (
    id BIGINT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_t4_t5 FOREIGN KEY (id) REFERENCES t5 (id)
);

CREATE TABLE t3 (
    id BIGINT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_t3_t4 FOREIGN KEY (id) REFERENCES t4 (id)
);

CREATE TABLE t2 (
    id BIGINT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_t2_t3 FOREIGN KEY (id) REFERENCES t3 (id)
);

CREATE TABLE t1 (
    id BIGINT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_t1_t2 FOREIGN KEY (id) REFERENCES t2 (id)
);

INSERT INTO t10 (id) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
INSERT INTO t9 (id) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
INSERT INTO t8 (id) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
INSERT INTO t7 (id) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
INSERT INTO t6 (id) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
INSERT INTO t5 (id) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
INSERT INTO t4 (id) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
INSERT INTO t3 (id) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
INSERT INTO t2 (id) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
INSERT INTO t1 (id) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
