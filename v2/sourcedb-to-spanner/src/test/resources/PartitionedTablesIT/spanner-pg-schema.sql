CREATE TABLE measurements_range (
    id bigint,
    city_id bigint,
    logdate date,
    peaktemp bigint,
    PRIMARY KEY (id, logdate)
);

CREATE TABLE employees_list (
    id bigint,
    name character varying(50),
    department character varying(50),
    PRIMARY KEY (id, department)
);

CREATE TABLE orders_hash (
    order_id bigint,
    customer_id bigint,
    amount bigint,
    PRIMARY KEY (order_id, customer_id)
);
