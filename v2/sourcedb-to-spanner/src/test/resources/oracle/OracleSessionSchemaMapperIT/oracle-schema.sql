CREATE TABLE company (
    company_id INTEGER PRIMARY KEY NOT NULL,
    company_name VARCHAR2(100) DEFAULT NULL,
    created_on DATE
)
-- SPLIT --
INSERT INTO company VALUES (1,'gog', TO_DATE('1998-09-04', 'YYYY-MM-DD'))
-- SPLIT --
INSERT INTO company VALUES (2,'app', TO_DATE('1976-04-01', 'YYYY-MM-DD'))
-- SPLIT --
INSERT INTO company VALUES (3,'ama', TO_DATE('1994-07-05', 'YYYY-MM-DD'))
-- SPLIT --
CREATE TABLE employee (
    employee_id INTEGER PRIMARY KEY NOT NULL,
    company_id INTEGER DEFAULT NULL,
    employee_name VARCHAR2(100) DEFAULT NULL,
    employee_address VARCHAR2(100) DEFAULT NULL,
    created_on DATE
)
-- SPLIT --
INSERT INTO employee VALUES (100,1,'emp1','add1', TO_DATE('1996-01-01', 'YYYY-MM-DD'))
-- SPLIT --
INSERT INTO employee VALUES (101,1,'emp2','add2', TO_DATE('1999-01-01', 'YYYY-MM-DD'))
-- SPLIT --
INSERT INTO employee VALUES (102,1,'emp3','add3', TO_DATE('2012-01-01', 'YYYY-MM-DD'))
-- SPLIT --
INSERT INTO employee VALUES (300,3,'emp300','add300', TO_DATE('1996-01-01', 'YYYY-MM-DD'))
-- SPLIT --
CREATE TABLE employee_attribute (
    employee_id INTEGER NOT NULL,
    attribute_name VARCHAR2(100) NOT NULL,
    value VARCHAR2(100) DEFAULT NULL,
    updated_on DATE,
    PRIMARY KEY (employee_id,attribute_name)
)
-- SPLIT --
INSERT INTO employee_attribute VALUES (100,'iq','150', TO_DATE('2024-06-10', 'YYYY-MM-DD'))
-- SPLIT --
INSERT INTO employee_attribute VALUES (101,'iq','120', TO_DATE('2024-06-10', 'YYYY-MM-DD'))
-- SPLIT --
INSERT INTO employee_attribute VALUES (102,'iq','20', TO_DATE('2024-06-10', 'YYYY-MM-DD'))
-- SPLIT --
INSERT INTO employee_attribute VALUES (300,'endurance','20', TO_DATE('2024-06-10', 'YYYY-MM-DD'))
-- SPLIT --
CREATE TABLE oracle_extra (
    test_id INTEGER PRIMARY KEY NOT NULL,
    test_name VARCHAR2(100) DEFAULT NULL
)
-- SPLIT --
CREATE VIEW company_view AS SELECT company_id FROM company
