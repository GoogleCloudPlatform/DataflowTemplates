CREATE TABLE "source_table1" (
    "id_col1" INTEGER PRIMARY KEY,
    "name_col1" VARCHAR2(255),
    "data_col1" CLOB
);

CREATE TABLE "source_table2" (
    "key_col2" VARCHAR2(50) PRIMARY KEY,
    "category_col2" VARCHAR2(100),
    "value_col2" CLOB
);

INSERT INTO "source_table1" ("id_col1", "name_col1", "data_col1") VALUES (1, 'Name One', 'Data for one');
INSERT INTO "source_table1" ("id_col1", "name_col1", "data_col1") VALUES (2, 'Name Two', 'Data for two');

INSERT INTO "source_table2" ("key_col2", "category_col2", "value_col2") VALUES ('K1', 'Category Alpha', 'Value Alpha');
INSERT INTO "source_table2" ("key_col2", "category_col2", "value_col2") VALUES ('K2', 'Category Beta', 'Value Beta');
