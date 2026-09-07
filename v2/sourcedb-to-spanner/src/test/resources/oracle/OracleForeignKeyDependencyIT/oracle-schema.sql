CREATE TABLE "t10" (
    "id" NUMBER(19,0) NOT NULL,
    PRIMARY KEY ("id")
)
-- SPLIT --
CREATE TABLE "t9" (
    "id" NUMBER(19,0) NOT NULL,
    PRIMARY KEY ("id"),
    CONSTRAINT "fk_t9_t10" FOREIGN KEY ("id") REFERENCES "t10" ("id")
)
-- SPLIT --
CREATE TABLE "t8" (
    "id" NUMBER(19,0) NOT NULL,
    PRIMARY KEY ("id"),
    CONSTRAINT "fk_t8_t9" FOREIGN KEY ("id") REFERENCES "t9" ("id")
)
-- SPLIT --
CREATE TABLE "t7" (
    "id" NUMBER(19,0) NOT NULL,
    PRIMARY KEY ("id"),
    CONSTRAINT "fk_t7_t8" FOREIGN KEY ("id") REFERENCES "t8" ("id")
)
-- SPLIT --
CREATE TABLE "t6" (
    "id" NUMBER(19,0) NOT NULL,
    PRIMARY KEY ("id"),
    CONSTRAINT "fk_t6_t7" FOREIGN KEY ("id") REFERENCES "t7" ("id")
)
-- SPLIT --
CREATE TABLE "t5" (
    "id" NUMBER(19,0) NOT NULL,
    PRIMARY KEY ("id"),
    CONSTRAINT "fk_t5_t6" FOREIGN KEY ("id") REFERENCES "t6" ("id")
)
-- SPLIT --
CREATE TABLE "t4" (
    "id" NUMBER(19,0) NOT NULL,
    PRIMARY KEY ("id"),
    CONSTRAINT "fk_t4_t5" FOREIGN KEY ("id") REFERENCES "t5" ("id")
)
-- SPLIT --
CREATE TABLE "t3" (
    "id" NUMBER(19,0) NOT NULL,
    PRIMARY KEY ("id"),
    CONSTRAINT "fk_t3_t4" FOREIGN KEY ("id") REFERENCES "t4" ("id")
)
-- SPLIT --
CREATE TABLE "t2" (
    "id" NUMBER(19,0) NOT NULL,
    PRIMARY KEY ("id"),
    CONSTRAINT "fk_t2_t3" FOREIGN KEY ("id") REFERENCES "t3" ("id")
)
-- SPLIT --
CREATE TABLE "t1" (
    "id" NUMBER(19,0) NOT NULL,
    PRIMARY KEY ("id"),
    CONSTRAINT "fk_t1_t2" FOREIGN KEY ("id") REFERENCES "t2" ("id")
)
-- SPLIT --
INSERT INTO "t10" ("id") VALUES (1)
-- SPLIT --
INSERT INTO "t9" ("id") VALUES (1)
-- SPLIT --
INSERT INTO "t8" ("id") VALUES (1)
-- SPLIT --
INSERT INTO "t7" ("id") VALUES (1)
-- SPLIT --
INSERT INTO "t6" ("id") VALUES (1)
-- SPLIT --
INSERT INTO "t5" ("id") VALUES (1)
-- SPLIT --
INSERT INTO "t4" ("id") VALUES (1)
-- SPLIT --
INSERT INTO "t3" ("id") VALUES (1)
-- SPLIT --
INSERT INTO "t2" ("id") VALUES (1)
-- SPLIT --
INSERT INTO "t1" ("id") VALUES (1)
-- SPLIT --
INSERT INTO "t10" ("id") VALUES (2)
-- SPLIT --
INSERT INTO "t9" ("id") VALUES (2)
-- SPLIT --
INSERT INTO "t8" ("id") VALUES (2)
-- SPLIT --
INSERT INTO "t7" ("id") VALUES (2)
-- SPLIT --
INSERT INTO "t6" ("id") VALUES (2)
-- SPLIT --
INSERT INTO "t5" ("id") VALUES (2)
-- SPLIT --
INSERT INTO "t4" ("id") VALUES (2)
-- SPLIT --
INSERT INTO "t3" ("id") VALUES (2)
-- SPLIT --
INSERT INTO "t2" ("id") VALUES (2)
-- SPLIT --
INSERT INTO "t1" ("id") VALUES (2)
-- SPLIT --
INSERT INTO "t10" ("id") VALUES (3)
-- SPLIT --
INSERT INTO "t9" ("id") VALUES (3)
-- SPLIT --
INSERT INTO "t8" ("id") VALUES (3)
-- SPLIT --
INSERT INTO "t7" ("id") VALUES (3)
-- SPLIT --
INSERT INTO "t6" ("id") VALUES (3)
-- SPLIT --
INSERT INTO "t5" ("id") VALUES (3)
-- SPLIT --
INSERT INTO "t4" ("id") VALUES (3)
-- SPLIT --
INSERT INTO "t3" ("id") VALUES (3)
-- SPLIT --
INSERT INTO "t2" ("id") VALUES (3)
-- SPLIT --
INSERT INTO "t1" ("id") VALUES (3)
-- SPLIT --
INSERT INTO "t10" ("id") VALUES (4)
-- SPLIT --
INSERT INTO "t9" ("id") VALUES (4)
-- SPLIT --
INSERT INTO "t8" ("id") VALUES (4)
-- SPLIT --
INSERT INTO "t7" ("id") VALUES (4)
-- SPLIT --
INSERT INTO "t6" ("id") VALUES (4)
-- SPLIT --
INSERT INTO "t5" ("id") VALUES (4)
-- SPLIT --
INSERT INTO "t4" ("id") VALUES (4)
-- SPLIT --
INSERT INTO "t3" ("id") VALUES (4)
-- SPLIT --
INSERT INTO "t2" ("id") VALUES (4)
-- SPLIT --
INSERT INTO "t1" ("id") VALUES (4)
-- SPLIT --
INSERT INTO "t10" ("id") VALUES (5)
-- SPLIT --
INSERT INTO "t9" ("id") VALUES (5)
-- SPLIT --
INSERT INTO "t8" ("id") VALUES (5)
-- SPLIT --
INSERT INTO "t7" ("id") VALUES (5)
-- SPLIT --
INSERT INTO "t6" ("id") VALUES (5)
-- SPLIT --
INSERT INTO "t5" ("id") VALUES (5)
-- SPLIT --
INSERT INTO "t4" ("id") VALUES (5)
-- SPLIT --
INSERT INTO "t3" ("id") VALUES (5)
-- SPLIT --
INSERT INTO "t2" ("id") VALUES (5)
-- SPLIT --
INSERT INTO "t1" ("id") VALUES (5)
-- SPLIT --
INSERT INTO "t10" ("id") VALUES (6)
-- SPLIT --
INSERT INTO "t9" ("id") VALUES (6)
-- SPLIT --
INSERT INTO "t8" ("id") VALUES (6)
-- SPLIT --
INSERT INTO "t7" ("id") VALUES (6)
-- SPLIT --
INSERT INTO "t6" ("id") VALUES (6)
-- SPLIT --
INSERT INTO "t5" ("id") VALUES (6)
-- SPLIT --
INSERT INTO "t4" ("id") VALUES (6)
-- SPLIT --
INSERT INTO "t3" ("id") VALUES (6)
-- SPLIT --
INSERT INTO "t2" ("id") VALUES (6)
-- SPLIT --
INSERT INTO "t1" ("id") VALUES (6)
-- SPLIT --
INSERT INTO "t10" ("id") VALUES (7)
-- SPLIT --
INSERT INTO "t9" ("id") VALUES (7)
-- SPLIT --
INSERT INTO "t8" ("id") VALUES (7)
-- SPLIT --
INSERT INTO "t7" ("id") VALUES (7)
-- SPLIT --
INSERT INTO "t6" ("id") VALUES (7)
-- SPLIT --
INSERT INTO "t5" ("id") VALUES (7)
-- SPLIT --
INSERT INTO "t4" ("id") VALUES (7)
-- SPLIT --
INSERT INTO "t3" ("id") VALUES (7)
-- SPLIT --
INSERT INTO "t2" ("id") VALUES (7)
-- SPLIT --
INSERT INTO "t1" ("id") VALUES (7)
-- SPLIT --
INSERT INTO "t10" ("id") VALUES (8)
-- SPLIT --
INSERT INTO "t9" ("id") VALUES (8)
-- SPLIT --
INSERT INTO "t8" ("id") VALUES (8)
-- SPLIT --
INSERT INTO "t7" ("id") VALUES (8)
-- SPLIT --
INSERT INTO "t6" ("id") VALUES (8)
-- SPLIT --
INSERT INTO "t5" ("id") VALUES (8)
-- SPLIT --
INSERT INTO "t4" ("id") VALUES (8)
-- SPLIT --
INSERT INTO "t3" ("id") VALUES (8)
-- SPLIT --
INSERT INTO "t2" ("id") VALUES (8)
-- SPLIT --
INSERT INTO "t1" ("id") VALUES (8)
-- SPLIT --
INSERT INTO "t10" ("id") VALUES (9)
-- SPLIT --
INSERT INTO "t9" ("id") VALUES (9)
-- SPLIT --
INSERT INTO "t8" ("id") VALUES (9)
-- SPLIT --
INSERT INTO "t7" ("id") VALUES (9)
-- SPLIT --
INSERT INTO "t6" ("id") VALUES (9)
-- SPLIT --
INSERT INTO "t5" ("id") VALUES (9)
-- SPLIT --
INSERT INTO "t4" ("id") VALUES (9)
-- SPLIT --
INSERT INTO "t3" ("id") VALUES (9)
-- SPLIT --
INSERT INTO "t2" ("id") VALUES (9)
-- SPLIT --
INSERT INTO "t1" ("id") VALUES (9)
-- SPLIT --
INSERT INTO "t10" ("id") VALUES (10)
-- SPLIT --
INSERT INTO "t9" ("id") VALUES (10)
-- SPLIT --
INSERT INTO "t8" ("id") VALUES (10)
-- SPLIT --
INSERT INTO "t7" ("id") VALUES (10)
-- SPLIT --
INSERT INTO "t6" ("id") VALUES (10)
-- SPLIT --
INSERT INTO "t5" ("id") VALUES (10)
-- SPLIT --
INSERT INTO "t4" ("id") VALUES (10)
-- SPLIT --
INSERT INTO "t3" ("id") VALUES (10)
-- SPLIT --
INSERT INTO "t2" ("id") VALUES (10)
-- SPLIT --
INSERT INTO "t1" ("id") VALUES (10)
