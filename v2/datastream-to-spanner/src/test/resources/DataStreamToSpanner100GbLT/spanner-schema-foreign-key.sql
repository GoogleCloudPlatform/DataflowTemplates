CREATE TABLE person1 (
     first_name1 STRING(500),
     last_name1 STRING(500),
     first_name2 STRING(500),
     last_name2 STRING(500),
     first_name3 STRING(500),
     last_name3 STRING(500),
     ID INT64 NOT NULL,
) PRIMARY KEY(ID);

CREATE TABLE person2 (
     first_name1 STRING(500),
     last_name1 STRING(500),
     first_name2 STRING(500),
     last_name2 STRING(500),
     first_name3 STRING(500),
     last_name3 STRING(500),
     FK_ID INT64 NOT NULL,
     CONSTRAINT FK_ID_CONS1 FOREIGN KEY (FK_ID) REFERENCES person1 (ID),
     ID INT64 NOT NULL,
) PRIMARY KEY(ID);

CREATE TABLE person3 (
     first_name1 STRING(500),
     last_name1 STRING(500),
     first_name2 STRING(500),
     last_name2 STRING(500),
     first_name3 STRING(500),
     last_name3 STRING(500),
     FK_ID INT64 NOT NULL,
     CONSTRAINT FK_ID_CONS2 FOREIGN KEY (FK_ID) REFERENCES person1 (ID),
     ID INT64 NOT NULL,
) PRIMARY KEY(ID);

CREATE TABLE person4 (
     first_name1 STRING(500),
     last_name1 STRING(500),
     first_name2 STRING(500),
     last_name2 STRING(500),
     first_name3 STRING(500),
     last_name3 STRING(500),
     FK_ID INT64 NOT NULL,
     CONSTRAINT FK_ID_CONS3 FOREIGN KEY (FK_ID) REFERENCES person1 (ID),
     ID INT64 NOT NULL,
) PRIMARY KEY(ID);

CREATE TABLE person5 (
     first_name1 STRING(500),
     last_name1 STRING(500),
     first_name2 STRING(500),
     last_name2 STRING(500),
     first_name3 STRING(500),
     last_name3 STRING(500),
     FK_ID INT64 NOT NULL,
     CONSTRAINT FK_ID_CONS4 FOREIGN KEY (FK_ID) REFERENCES person1 (ID),
     ID INT64 NOT NULL,
) PRIMARY KEY(ID);