CREATE TABLE "Category" (
  "category_id" NUMBER NOT NULL PRIMARY KEY,
  "full_name" VARCHAR2(25),
  "last_update" TIMESTAMP
);

CREATE TABLE "Books" (
   "id" NUMBER NOT NULL,
   "title" VARCHAR2(200),
   "author_id" NUMBER
);
