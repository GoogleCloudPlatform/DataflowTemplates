CREATE TABLE "person1" (
  "first_name1" VARCHAR2(500),
  "last_name1" VARCHAR2(500),
  "first_name2" VARCHAR2(500),
  "last_name2" VARCHAR2(500),
  "first_name3" VARCHAR2(500),
  "last_name3" VARCHAR2(500),
  "ID" NUMBER NOT NULL PRIMARY KEY
);
ALTER TABLE "person1" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
