CREATE TABLE parent1 ( id INT NOT NULL, update_ts timestamp DEFAULT NULL,
 in_ts timestamp  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (id) );


CREATE TABLE child11 ( child_id INT, parent_id INT,  update_ts timestamp DEFAULT NULL,
 in_ts timestamp  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent1(id),PRIMARY KEY (child_id) );


CREATE TABLE parent2 ( id INT NOT NULL,  update_ts timestamp DEFAULT NULL,
 in_ts timestamp  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (id) );


CREATE TABLE child21 ( child_id INT, parent_id INT,  update_ts timestamp DEFAULT NULL,
 in_ts timestamp  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent2(id),PRIMARY KEY (child_id) );

CREATE TABLE child31 ( child_id INT, parent_id INT,  update_ts timestamp DEFAULT NULL,
    in_ts timestamp  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX par_ind2 (parent_id), FOREIGN KEY (parent_id) REFERENCES parent2(id),PRIMARY KEY (child_id) );