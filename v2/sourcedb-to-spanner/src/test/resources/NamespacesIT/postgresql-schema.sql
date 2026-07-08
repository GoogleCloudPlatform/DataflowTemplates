CREATE SCHEMA "my-namespace";

CREATE TABLE "my-namespace"."singers" (
  "singer_id" int8 PRIMARY KEY,
  "first_name" varchar(1024)
);

CREATE TABLE "my-namespace"."albums" (
  "singer_id" int8 NOT NULL,
  "album_id" int8 NOT NULL,
  "album_serial_number" int8,
  PRIMARY KEY ("singer_id", "album_id"),
  CONSTRAINT "album_id_fk" FOREIGN KEY ("album_id") REFERENCES "my-namespace"."singers" ("singer_id")
);

CREATE INDEX "album_serial_number_idx"
ON "my-namespace"."albums" ("album_serial_number");

INSERT INTO "my-namespace"."singers" ("singer_id", "first_name")
VALUES
  (1, 'Singer 1'),
  (2, 'Singer 2');

INSERT INTO "my-namespace"."albums" ("singer_id", "album_id", "album_serial_number")
VALUES
  (1, 1, 10),
  (1, 2, 11),
  (2, 2, 20);
