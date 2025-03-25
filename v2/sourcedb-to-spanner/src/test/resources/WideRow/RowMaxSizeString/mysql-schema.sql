CREATE TABLE IF NOT EXISTS WideRowTable (
    id BIGINT NOT NULL,
    max_string_col LONGTEXT,
    PRIMARY KEY (id)
);

INSERT INTO WideRowTable (id, max_string_col) VALUES (1, REPEAT('a', 2621440));
INSERT INTO WideRowTable (id, max_string_col) VALUES (2, REPEAT('b', 2621440));
INSERT INTO WideRowTable (id, max_string_col) VALUES (3, REPEAT('c', 2621440));
INSERT INTO WideRowTable (id, max_string_col) VALUES (4, REPEAT('d', 2621440));