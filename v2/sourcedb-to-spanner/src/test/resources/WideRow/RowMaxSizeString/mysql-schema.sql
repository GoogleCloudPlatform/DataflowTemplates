CREATE TABLE IF NOT EXISTS WideRowTable (
    id BIGINT NOT NULL,
    max_string_col LONGTEXT,
    PRIMARY KEY (id)
);

INSERT INTO WideRowTable (id, max_string_col) VALUES (1, REPEAT('a', 2621440));