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

CREATE VIEW company_view SQL SECURITY DEFINER AS SELECT company.company_id FROM  company;