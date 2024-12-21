CREATE TABLE
    company
(
    company_id      INT64 NOT NULL,
    company_name    STRING(100),
    created_on      DATE,
) PRIMARY KEY
  (company_id);
CREATE TABLE
    employee
(
    employee_id         INT64 NOT NULL,
    company_id          INT64,
    employee_name       STRING(100),
    employee_address    STRING(100),
    created_on          DATE,
) PRIMARY KEY
  (employee_id);

CREATE TABLE
    employee_attribute
(
    employee_id    INT64 NOT NULL,
    attribute_name STRING(100) NOT NULL,
    value          STRING(100),
    updated_on     DATE,
) PRIMARY KEY
  (employee_id, attribute_name);

CREATE SEQUENCE Sequence7 OPTIONS (sequence_kind = 'bit_reversed_positive');

CREATE TABLE vendor (
    vendor_id INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE Sequence7)),
    first_name STRING(255) NOT NULL,
    last_name STRING(255) NOT NULL,
    email STRING(255) NOT NULL,
    full_name STRING(512),
) PRIMARY KEY (full_name);

CREATE INDEX full_name_idx ON vendor (full_name);
CREATE INDEX email_idx ON vendor (email DESC);

CREATE VIEW company_view SQL SECURITY DEFINER AS SELECT company.company_id FROM  company;