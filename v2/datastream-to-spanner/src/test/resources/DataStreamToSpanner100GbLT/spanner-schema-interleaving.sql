CREATE TABLE person1 (
     first_name1 STRING(500),
     last_name1 STRING(500),
     first_name2 STRING(500),
     last_name2 STRING(500),
     first_name3 STRING(500),
     last_name3 STRING(500),
     IL_ID INT64 NOT NULL,
) PRIMARY KEY(IL_ID);

CREATE TABLE person2 (
     first_name1 STRING(500),
     last_name1 STRING(500),
     first_name2 STRING(500),
     last_name2 STRING(500),
     first_name3 STRING(500),
     last_name3 STRING(500),
     IL_ID INT64 NOT NULL,
     ID INT64 NOT NULL,
) PRIMARY KEY(IL_ID, ID),
    INTERLEAVE IN PARENT person1 ON DELETE CASCADE;

CREATE TABLE person3 (
     first_name1 STRING(500),
     last_name1 STRING(500),
     first_name2 STRING(500),
     last_name2 STRING(500),
     first_name3 STRING(500),
     last_name3 STRING(500),
     IL_ID INT64 NOT NULL,
     ID INT64 NOT NULL,
) PRIMARY KEY(IL_ID, ID),
    INTERLEAVE IN PARENT person1 ON DELETE CASCADE;

CREATE TABLE person4 (
     first_name1 STRING(500),
     last_name1 STRING(500),
     first_name2 STRING(500),
     last_name2 STRING(500),
     first_name3 STRING(500),
     last_name3 STRING(500),
     IL_ID INT64 NOT NULL,
     ID INT64 NOT NULL,
) PRIMARY KEY(IL_ID, ID),
    INTERLEAVE IN PARENT person1 ON DELETE CASCADE;

CREATE TABLE person5 (
     first_name1 STRING(500),
     last_name1 STRING(500),
     first_name2 STRING(500),
     last_name2 STRING(500),
     first_name3 STRING(500),
     last_name3 STRING(500),
     IL_ID INT64 NOT NULL,
     ID INT64 NOT NULL,
) PRIMARY KEY(IL_ID, ID),
    INTERLEAVE IN PARENT person1 ON DELETE CASCADE;