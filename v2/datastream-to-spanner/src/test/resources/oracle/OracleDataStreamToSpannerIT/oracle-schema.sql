CREATE TABLE "DatastreamToSpanner_1" (
  "row_id" NUMBER NOT NULL,
  "name" VARCHAR2(200),
  "age" NUMBER,
  "member" VARCHAR2(200),
  "entry_added" VARCHAR2(200),
  PRIMARY KEY ("row_id")
);

ALTER TABLE "DatastreamToSpanner_1" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

CREATE TABLE "DatastreamToSpanner_2" (
  "row_id" NUMBER NOT NULL,
  "name" VARCHAR2(200),
  "age" NUMBER,
  "member" VARCHAR2(200),
  "entry_added" VARCHAR2(200),
  PRIMARY KEY ("row_id")
);

ALTER TABLE "DatastreamToSpanner_2" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
