CREATE TABLE parent1 (
    id INT NOT NULL,
    update_ts DATETIME NULL,
    in_ts DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

CREATE TABLE child11 (
    child_id INT NOT NULL,
    parent_id INT,
    update_ts DATETIME NULL,
    in_ts DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (child_id),
    FOREIGN KEY (parent_id) REFERENCES parent1(id) ON DELETE CASCADE
);

CREATE TABLE parent2 (
    id INT NOT NULL,
    update_ts DATETIME NULL,
    in_ts DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

CREATE TABLE child21 (
    child_id INT NOT NULL,
    parent_id INT,
    update_ts DATETIME NULL,
    in_ts DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (child_id),
    FOREIGN KEY (parent_id) REFERENCES parent2(id) ON DELETE CASCADE
);

CREATE TABLE child31 (
    child_id INT NOT NULL,
    parent_id INT,
    update_ts DATETIME NULL,
    in_ts DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (child_id),
    FOREIGN KEY (parent_id) REFERENCES parent2(id) ON DELETE CASCADE
);
