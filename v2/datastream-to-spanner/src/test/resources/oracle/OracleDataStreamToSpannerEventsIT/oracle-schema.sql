CREATE TABLE "Movie" (
    "id" INTEGER NOT NULL,
    "name" VARCHAR2(200),
    "actor" NUMBER,
    "startTime" TIMESTAMP,
    PRIMARY KEY ("id")
);

CREATE TABLE "Users" (
    "id" INTEGER NOT NULL,
    "name" VARCHAR2(200),
    "age" INTEGER,
    "subscribed" NUMBER(1),
    "plan" CHAR(1),
    "startDate" DATE,
    PRIMARY KEY ("id")
);

CREATE TABLE "Authors" (
    "author_id" INTEGER NOT NULL,
    "name" VARCHAR2(200),
    PRIMARY KEY ("author_id")
);

CREATE TABLE "Articles" (
    "id" INTEGER NOT NULL,
    "name" VARCHAR2(200),
    "published_date" DATE,
    "author_id" INTEGER,
    PRIMARY KEY ("id")
);

ALTER TABLE "Articles" add FOREIGN KEY ("author_id") references "Authors"("author_id");
CREATE INDEX "author_id" ON "Articles" ("author_id");

CREATE TABLE "Books" (
    "id" INTEGER NOT NULL,
    "title" VARCHAR2(200),
    "author_id" INTEGER,
    PRIMARY KEY ("id")
);

ALTER TABLE "Books" add FOREIGN KEY ("author_id") references "Authors"("author_id");
CREATE INDEX "author_id_6" ON "Books" ("author_id");
