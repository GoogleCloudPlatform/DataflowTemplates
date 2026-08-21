CREATE TABLE "DateData" (
    "id" INTEGER NOT NULL,
    "timestamp_column" TIMESTAMP WITH LOCAL TIME ZONE,
    "datetime_column" TIMESTAMP,
 PRIMARY KEY("id"));

ALTER SESSION SET TIME_ZONE = 'Australia/Brisbane';

INSERT INTO "DateData" ("id", "timestamp_column", "datetime_column") VALUES (1, TIMESTAMP '2024-02-02 10:00:00.0', TIMESTAMP '2024-02-02 10:00:00.0');
INSERT INTO "DateData" ("id", "timestamp_column", "datetime_column") VALUES (2, TIMESTAMP '2024-02-02 20:00:00.0', TIMESTAMP '2024-02-02 20:00:00.0');
INSERT INTO "DateData" ("id", "timestamp_column", "datetime_column") VALUES (3, TIMESTAMP '2024-02-03 06:00:00.0', TIMESTAMP '2024-02-03 06:00:00.0');
