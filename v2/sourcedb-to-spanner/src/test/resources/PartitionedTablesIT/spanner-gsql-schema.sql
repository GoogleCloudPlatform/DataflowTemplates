CREATE TABLE measurements_range (
    id INT64,
    city_id INT64,
    logdate DATE,
    peaktemp INT64
) PRIMARY KEY(id, logdate);

CREATE TABLE employees_list (
    id INT64,
    name STRING(50),
    department STRING(50)
) PRIMARY KEY(id, department);

CREATE TABLE orders_hash (
    order_id INT64,
    customer_id INT64,
    amount INT64
) PRIMARY KEY(order_id, customer_id);
