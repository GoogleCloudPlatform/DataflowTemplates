CREATE TABLE employees (
  id INT64 NOT NULL,
  first_name STRING(50) NOT NULL,
  last_name STRING(50) NOT NULL,
  department STRING(50) NOT NULL,
  salary FLOAT64 NOT NULL,
  hire_date DATE NOT NULL
) PRIMARY KEY (id);
