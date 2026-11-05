CREATE TABLE "company" (
    "company_id" NUMBER PRIMARY KEY NOT NULL,
    "company_name" VARCHAR2(100) DEFAULT NULL,
    "created_on" DATE
);

INSERT INTO "company" ("company_id", "company_name", "created_on") VALUES (1, 'gog', DATE '1998-09-04');
INSERT INTO "company" ("company_id", "company_name", "created_on") VALUES (2, 'app', DATE '1976-04-01');
INSERT INTO "company" ("company_id", "company_name", "created_on") VALUES (3, 'ama', DATE '1994-07-05');

CREATE TABLE "employee" (
    "employee_id" NUMBER PRIMARY KEY NOT NULL,
    "company_id" NUMBER DEFAULT NULL,
    "employee_name" VARCHAR2(100) DEFAULT NULL,
    "employee_address" VARCHAR2(100) DEFAULT NULL,
    "created_on" DATE
);

INSERT INTO "employee" ("employee_id", "company_id", "employee_name", "employee_address", "created_on") VALUES (100, 1, 'emp1', 'add1', DATE '1996-01-01');
INSERT INTO "employee" ("employee_id", "company_id", "employee_name", "employee_address", "created_on") VALUES (101, 1, 'emp2', 'add2', DATE '1999-01-01');
INSERT INTO "employee" ("employee_id", "company_id", "employee_name", "employee_address", "created_on") VALUES (102, 1, 'emp3', 'add3', DATE '2012-01-01');
INSERT INTO "employee" ("employee_id", "company_id", "employee_name", "employee_address", "created_on") VALUES (300, 3, 'emp300', 'add300', DATE '1996-01-01');

CREATE TABLE "employee_attribute" (
    "employee_id" NUMBER NOT NULL,
    "attribute_name" VARCHAR2(100) NOT NULL,
    "value" VARCHAR2(100) DEFAULT NULL,
    "updated_on" DATE,
    PRIMARY KEY ("employee_id", "attribute_name")
);

INSERT INTO "employee_attribute" ("employee_id", "attribute_name", "value", "updated_on") VALUES (100, 'iq', '150', DATE '2024-06-10');
INSERT INTO "employee_attribute" ("employee_id", "attribute_name", "value", "updated_on") VALUES (101, 'iq', '120', DATE '2024-06-10');
INSERT INTO "employee_attribute" ("employee_id", "attribute_name", "value", "updated_on") VALUES (102, 'iq', '20', DATE '2024-06-10');
INSERT INTO "employee_attribute" ("employee_id", "attribute_name", "value", "updated_on") VALUES (300, 'endurance', '20', DATE '2024-06-10');

CREATE TABLE "oracle_extra" (
    "test_id" NUMBER PRIMARY KEY NOT NULL,
    "test_name" VARCHAR2(100) DEFAULT NULL
);

CREATE OR REPLACE VIEW "company_view" AS SELECT "company_id" FROM "company";
