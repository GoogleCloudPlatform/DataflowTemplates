CREATE TABLE "Authors" (
    "author_id" NUMBER(10,0) NOT NULL,
    "name" VARCHAR2(25),
    PRIMARY KEY ("author_id")
);

CREATE TABLE "Books" (
    "id" NUMBER(10,0) NOT NULL,
    "title" VARCHAR2(200),
    "author_id" NUMBER(10,0),
    PRIMARY KEY ("id")
);

ALTER TABLE "Books" ADD CONSTRAINT "Books_ibfk_1" FOREIGN KEY ("author_id") REFERENCES "Authors" ("author_id");

CREATE TABLE "Genre" (
    "name" VARCHAR2(200),
    "genre_id" NUMBER(10,0) NOT NULL,
    PRIMARY KEY ("genre_id")
);
