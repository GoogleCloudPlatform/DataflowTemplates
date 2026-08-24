CREATE TABLE "LargeKey" (
    "pk_col1" VARCHAR2(255) NOT NULL,
    "pk_col2" VARCHAR2(255) NOT NULL,
    "pk_col3" VARCHAR2(255) NOT NULL,
    "col1" VARCHAR2(255),
    "col2" VARCHAR2(255),
    "col3" VARCHAR2(255),
    "value_col" CLOB,
    PRIMARY KEY ("pk_col1", "pk_col2", "pk_col3")
);

CREATE INDEX "large_index" ON "LargeKey" ("col1", "col2", "col3");

INSERT INTO "LargeKey" VALUES (
    RPAD('A', 255, 'A'),
    RPAD('B', 255, 'B'),
    RPAD('C', 255, 'C'),
    RPAD('A', 255, 'A'),
    RPAD('B', 255, 'B'),
    RPAD('C', 255, 'C'),
    '3072 bytes of total size of table key as per the mysql limitation'
);

CREATE TABLE "LargeCell" (
  "id" NUMBER PRIMARY KEY,
  "max_string_col_to_bytes" BLOB,
  "max_string_col_to_str" CLOB
);
