CREATE TABLE measurements_range (
    id BIGINT,
    city_id BIGINT,
    logdate DATE,
    peaktemp BIGINT,
    PRIMARY KEY (id, logdate)
);

CREATE TABLE employees_list (
    id BIGINT,
    name VARCHAR(50),
    department VARCHAR(50),
    PRIMARY KEY (id, department)
);

CREATE TABLE orders_hash (
    order_id BIGINT,
    customer_id BIGINT,
    amount BIGINT,
    PRIMARY KEY (order_id, customer_id)
);
