CREATE TABLE "DateData" (
  "id" NUMBER PRIMARY KEY,
  "timestamp_column" TIMESTAMP,
  "datetime_column" TIMESTAMP
);

ALTER TABLE "DateData" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
