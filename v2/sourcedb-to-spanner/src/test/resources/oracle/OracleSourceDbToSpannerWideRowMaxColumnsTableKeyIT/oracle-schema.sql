CREATE TABLE "LargePrimaryKeyTable" (
    "pk_col1" VARCHAR2(48) NOT NULL,
    "pk_col2" VARCHAR2(48) NOT NULL,
    "pk_col3" VARCHAR2(48) NOT NULL,
    "pk_col4" VARCHAR2(48) NOT NULL,
    "pk_col5" VARCHAR2(48) NOT NULL,
    "pk_col6" VARCHAR2(48) NOT NULL,
    "pk_col7" VARCHAR2(48) NOT NULL,
    "pk_col8" VARCHAR2(48) NOT NULL,
    "pk_col9" VARCHAR2(48) NOT NULL,
    "pk_col10" VARCHAR2(48) NOT NULL,
    "pk_col11" VARCHAR2(48) NOT NULL,
    "pk_col12" VARCHAR2(48) NOT NULL,
    "pk_col13" VARCHAR2(48) NOT NULL,
    "pk_col14" VARCHAR2(48) NOT NULL,
    "pk_col15" VARCHAR2(48) NOT NULL,
    "pk_col16" VARCHAR2(48) NOT NULL,
    "value_col" CLOB,
    PRIMARY KEY (
        "pk_col1", "pk_col2", "pk_col3", "pk_col4", "pk_col5",
        "pk_col6", "pk_col7", "pk_col8", "pk_col9", "pk_col10",
        "pk_col11", "pk_col12", "pk_col13", "pk_col14", "pk_col15", "pk_col16"
    )
)

-- SPLIT --

INSERT INTO "LargePrimaryKeyTable" (
    "pk_col1", "pk_col2", "pk_col3", "pk_col4", "pk_col5", "pk_col6", "pk_col7", "pk_col8",
    "pk_col9", "pk_col10", "pk_col11", "pk_col12", "pk_col13", "pk_col14", "pk_col15", "pk_col16",
    "value_col"
) VALUES (
    RPAD('A', 48, 'A'), RPAD('B', 48, 'B'), RPAD('C', 48, 'C'), RPAD('D', 48, 'D'),
    RPAD('E', 48, 'E'), RPAD('F', 48, 'F'), RPAD('G', 48, 'G'), RPAD('H', 48, 'H'),
    RPAD('I', 48, 'I'), RPAD('J', 48, 'J'), RPAD('K', 48, 'K'), RPAD('L', 48, 'L'),
    RPAD('M', 48, 'M'), RPAD('N', 48, 'N'), RPAD('O', 48, 'O'), RPAD('P', 48, 'P'),
    '3072 Bytes of total size of table key'
)
