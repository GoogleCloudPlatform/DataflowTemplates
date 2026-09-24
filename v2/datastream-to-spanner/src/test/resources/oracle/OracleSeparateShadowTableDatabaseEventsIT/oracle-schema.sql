CREATE TABLE "Movie" (
    "id" NUMBER NOT NULL,
    "name" VARCHAR2(200),
    "actor" NUMBER,
    "startTime" TIMESTAMP,
    PRIMARY KEY ("id")
);

CREATE TABLE "Users" (
    "id" NUMBER NOT NULL,
    "name" VARCHAR2(200),
    "age" NUMBER,
    "subscribed" NUMBER(1),
    "plan" CHAR(1),
    "startDate" DATE,
    PRIMARY KEY ("id")
);

CREATE TABLE "Authors" (
    "author_id" NUMBER NOT NULL,
    "name" VARCHAR2(200),
    PRIMARY KEY ("author_id")
);

CREATE TABLE "Articles" (
    "id" NUMBER NOT NULL,
    "name" VARCHAR2(200),
    "published_date" DATE,
    "author_id" NUMBER,
    PRIMARY KEY ("id")
);

ALTER TABLE "Articles" ADD FOREIGN KEY ("author_id") REFERENCES "Authors"("author_id");

CREATE INDEX author_id ON "Articles" ("author_id");

CREATE TABLE "Books" (
    "id" NUMBER NOT NULL,
    "title" VARCHAR2(200),
    "author_id" NUMBER,
    PRIMARY KEY ("id")
);

ALTER TABLE "Books" ADD FOREIGN KEY ("author_id") REFERENCES "Authors"("author_id");

CREATE INDEX author_id_6 ON "Books" ("author_id");

ALTER TABLE "Movie" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE "Users" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE "Authors" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE "Articles" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE "Books" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
