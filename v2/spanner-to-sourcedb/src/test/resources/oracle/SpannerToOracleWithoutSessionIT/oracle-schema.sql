CREATE TABLE "generated_pk_column_table" (
	"first_name_col" VARCHAR2(50) DEFAULT NULL,
	"last_name_col" VARCHAR2(50) DEFAULT NULL,
	"generated_column_col" VARCHAR2(100) GENERATED ALWAYS AS (CONCAT("first_name_col", ' ')) VIRTUAL NOT NULL,
	PRIMARY KEY ("generated_column_col")
);

CREATE TABLE "generated_non_pk_column_table" ( 
	"first_name_col" VARCHAR2(50) DEFAULT NULL,
	"last_name_col" VARCHAR2(50) DEFAULT NULL,
	"generated_column_col" VARCHAR2(100) GENERATED ALWAYS AS (CONCAT("first_name_col", ' ')) VIRTUAL NOT NULL,
  "id" INT not null,
	PRIMARY KEY ("id")
);

CREATE TABLE "non_generated_to_generated_column_table" ( 
	"first_name_col" VARCHAR2(50) DEFAULT NULL,
	"last_name_col" VARCHAR2(50) DEFAULT NULL,
  "generated_column_col" VARCHAR2(100) NOT NULL,
	"generated_column_pk_col" VARCHAR2(100) NOT NULL,
	PRIMARY KEY ("generated_column_pk_col")
);

CREATE TABLE "generated_to_non_generated_column_table" ( 
	"first_name_col" VARCHAR2(50) DEFAULT NULL,
	"last_name_col" VARCHAR2(50) DEFAULT NULL,
  "generated_column_col" VARCHAR2(100) GENERATED ALWAYS AS (CONCAT("first_name_col", ' ')) VIRTUAL NOT NULL,
	"generated_column_pk_col" VARCHAR2(100) GENERATED ALWAYS AS (CASE WHEN "first_name_col" IS NOT NULL THEN "first_name_col" END || ' ') VIRTUAL NOT NULL,
	PRIMARY KEY ("generated_column_pk_col")
);
