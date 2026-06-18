-- Tables with interleaving
CREATE TABLE "Users" (
	"first_name"                            character varying,
	"last_name"                             character varying(5),
	"age"                                   bigint,
	PRIMARY KEY ("first_name", "last_name")
);

-- All Datatypes
CREATE TABLE "AllTYPES" (
	"id"                                    bigint NOT NULL,
	"first_name"                            character varying,
	"last_name"                             character varying(5),
	"bool_field"                            boolean,
	"int_field"                             bigint,
	"float32_field"                         real,
	"float64_field"                         double precision,
	"string_field"                          text,
	"bytes_field"                           bytea,
	"timestamp_field"                       timestamp with time zone,
	"numeric_field"                         numeric,
	"date_field"                            date,
	"arr_bool_field"                        boolean[],
	"arr_int_field"                         bigint[],
	"arr_float32_field"                     real[],
	"arr_float64_field"                     double precision[],
	"arr_string_field"                      character varying[],
	"arr_bytes_field"                       bytea[],
	"arr_timestamp_field"                   timestamp with time zone[],
	"arr_date_field"                        date[],
	"arr_numeric_field"                     numeric[],
	PRIMARY KEY ("first_name", "last_name", "id", "float64_field")
) 
INTERLEAVE IN PARENT "Users" ON DELETE CASCADE;

-- Foreign Keys
CREATE TABLE "Ref" (
	"id1"                                   bigint,
	"id2"                                   bigint,
	PRIMARY KEY ("id1", "id2")
);
CREATE TABLE "Child" (
	"id1"                                   bigint,
	"id2"                                   bigint,
	"id3"                                   bigint,
	PRIMARY KEY ("id1", "id2", "id3")
) 
INTERLEAVE IN PARENT "Ref";
ALTER TABLE "Child" ADD CONSTRAINT "fk1" FOREIGN KEY ("id1") REFERENCES "Ref" ("id1");
ALTER TABLE "Child" ADD CONSTRAINT "fk2" FOREIGN KEY ("id2") REFERENCES "Ref" ("id2");
ALTER TABLE "Child" ADD CONSTRAINT "fk3" FOREIGN KEY ("id2") REFERENCES "Ref" ("id2");
ALTER TABLE "Child" ADD CONSTRAINT "fk4" FOREIGN KEY ("id2", "id1") REFERENCES "Ref" ("id2", "id1");
