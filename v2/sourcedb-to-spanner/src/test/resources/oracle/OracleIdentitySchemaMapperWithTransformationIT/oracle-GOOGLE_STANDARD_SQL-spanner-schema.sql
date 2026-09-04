CREATE TABLE IF NOT EXISTS
    company
(
    company_id      INT64 NOT NULL,
    company_name    STRING(100),
    created_on      STRING(100),
) PRIMARY KEY
  (company_id);
CREATE TABLE IF NOT EXISTS
    employee_sp
(
    employee_id         INT64 NOT NULL,
    company_id          INT64,
    employee_name       STRING(100),
    employee_address_sp STRING(100),
    created_on          DATE,
) PRIMARY KEY
  (employee_id);

CREATE TABLE IF NOT EXISTS
    employee_attribute
(
    employee_id    INT64 NOT NULL,
    attribute_name STRING(100) NOT NULL,
    value          STRING(100),
    updated_on     DATE,
) PRIMARY KEY
  (employee_id, attribute_name);
