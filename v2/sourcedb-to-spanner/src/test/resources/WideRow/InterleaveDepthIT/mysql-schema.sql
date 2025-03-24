CREATE TABLE parent (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE child1 (
    id INT PRIMARY KEY,
    parent_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE
);

CREATE TABLE child2 (
    id INT PRIMARY KEY,
    child1_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    FOREIGN KEY (child1_id) REFERENCES child1(id) ON DELETE CASCADE
);

CREATE TABLE child3 (
    id INT PRIMARY KEY,
    child2_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    FOREIGN KEY (child2_id) REFERENCES child2(id) ON DELETE CASCADE
);

CREATE TABLE child4 (
    id INT PRIMARY KEY,
    child3_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    FOREIGN KEY (child3_id) REFERENCES child3(id) ON DELETE CASCADE
);

CREATE TABLE child5 (
    id INT PRIMARY KEY,
    child4_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    FOREIGN KEY (child4_id) REFERENCES child4(id) ON DELETE CASCADE
);

CREATE TABLE child6 (
    id INT PRIMARY KEY,
    child5_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    FOREIGN KEY (child5_id) REFERENCES child5(id) ON DELETE CASCADE
);

CREATE TABLE child7 (
    id INT PRIMARY KEY,
    child6_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    FOREIGN KEY (child6_id) REFERENCES child6(id) ON DELETE CASCADE
);



INSERT INTO parent (id, name) VALUES (1, 'Root');
INSERT INTO child1 (id, parent_id, name) VALUES (1, 1, 'Child Level 1');
INSERT INTO child2 (id, child1_id, name) VALUES (1, 1, 'Child Level 2');
INSERT INTO child3 (id, child2_id, name) VALUES (1, 1, 'Child Level 3');
INSERT INTO child4 (id, child3_id, name) VALUES (1, 1, 'Child Level 4');
INSERT INTO child5 (id, child4_id, name) VALUES (1, 1, 'Child Level 5');
INSERT INTO child6 (id, child5_id, name) VALUES (1, 1, 'Child Level 6');
INSERT INTO child7 (id, child6_id, name) VALUES (1, 1, 'Child Level 7');
