CREATE TABLE Authors (
    author_id INT64 NOT NULL ,
    full_name STRING(25),
) PRIMARY KEY (author_id);

CREATE TABLE Books (
    id INT64 NOT NULL ,
    title STRING(200),
    author_id INT64,
    synth_id STRING(50),
) PRIMARY KEY (synth_id);

CREATE INDEX author_id ON Books (author_id);

ALTER TABLE Books ADD CONSTRAINT Books_ibfk_1 FOREIGN KEY (author_id) REFERENCES Authors (author_id) ON DELETE NO ACTION;