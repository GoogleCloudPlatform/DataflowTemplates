CREATE TABLE employees (
    id INT64 NOT NULL,
    first_name STRING(50) NOT NULL,
    last_name STRING(50) NOT NULL,
    department STRING(50) NOT NULL,
    salary FLOAT64 NOT NULL,
    hire_date DATE NOT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
) PRIMARY KEY (id, end_date);
