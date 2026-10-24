CREATE USER "my-namespace" IDENTIFIED BY "testpassword";
ALTER USER "my-namespace" QUOTA UNLIMITED ON USERS;

CREATE TABLE "my-namespace"."singers" (
  "singer_id" NUMBER PRIMARY KEY,
  "first_name" VARCHAR2(1024)
);

CREATE TABLE "my-namespace"."albums" (
  "singer_id" NUMBER NOT NULL,
  "album_id" NUMBER NOT NULL,
  "album_serial_number" NUMBER,
  PRIMARY KEY ("singer_id", "album_id"),
  CONSTRAINT "album_id_fk" FOREIGN KEY ("album_id") REFERENCES "my-namespace"."singers" ("singer_id")
);

CREATE INDEX "album_serial_number_idx"
ON "my-namespace"."albums" ("album_serial_number");

INSERT INTO "my-namespace"."singers" ("singer_id", "first_name") VALUES (1, 'Singer 1');
INSERT INTO "my-namespace"."singers" ("singer_id", "first_name") VALUES (2, 'Singer 2');

INSERT INTO "my-namespace"."albums" ("singer_id", "album_id", "album_serial_number") VALUES (1, 1, 10);
INSERT INTO "my-namespace"."albums" ("singer_id", "album_id", "album_serial_number") VALUES (1, 2, 11);
INSERT INTO "my-namespace"."albums" ("singer_id", "album_id", "album_serial_number") VALUES (2, 2, 20);
