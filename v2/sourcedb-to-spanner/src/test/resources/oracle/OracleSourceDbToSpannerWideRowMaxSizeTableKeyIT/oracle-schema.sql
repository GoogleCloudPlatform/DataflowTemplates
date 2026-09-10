CREATE TABLE "LargePrimaryKeyTable" (
"pk_col1" VARCHAR2(255) NOT NULL,
"pk_col2" VARCHAR2(255) NOT NULL,
"pk_col3" VARCHAR2(255) NOT NULL,
"value_col" CLOB,
PRIMARY KEY ("pk_col1", "pk_col2", "pk_col3")
)
-- SPLIT --
INSERT INTO "LargePrimaryKeyTable" (
    "pk_col1", "pk_col2", "pk_col3","value_col"
) VALUES (
    RPAD('A', 255, 'A'),
    RPAD('B', 255, 'B'),
    RPAD('C', 255, 'C'),
    '3072 bytes of total size of table key as per the mysql limitation'
)
