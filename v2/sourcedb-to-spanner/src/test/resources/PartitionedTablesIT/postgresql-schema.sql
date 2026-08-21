CREATE TABLE measurements_range (
    id INT,
    city_id INT NOT NULL,
    logdate DATE NOT NULL,
    peaktemp INT,
    PRIMARY KEY (id, logdate)
) PARTITION BY RANGE (logdate);

CREATE TABLE measurements_range_y2006m02 PARTITION OF measurements_range
    FOR VALUES FROM ('2006-02-01') TO ('2006-03-01');

CREATE TABLE measurements_range_y2006m03 PARTITION OF measurements_range
    FOR VALUES FROM ('2006-03-01') TO ('2006-04-01');

CREATE TABLE measurements_range_default PARTITION OF measurements_range DEFAULT;

INSERT INTO measurements_range (id, city_id, logdate, peaktemp) VALUES (1, 1, '2006-02-15', 33);
INSERT INTO measurements_range (id, city_id, logdate, peaktemp) VALUES (2, 2, '2006-03-15', 35);
INSERT INTO measurements_range (id, city_id, logdate, peaktemp) VALUES (3, 3, '2006-05-10', 40);
INSERT INTO measurements_range_y2006m02 (id, city_id, logdate, peaktemp) VALUES (4, 4, '2006-02-20', 30);

CREATE TABLE employees_list (
    id INT,
    name VARCHAR(50),
    department VARCHAR(50),
    PRIMARY KEY (id, department)
) PARTITION BY LIST (department);

CREATE TABLE employees_list_engineering PARTITION OF employees_list FOR VALUES IN ('Engineering');
CREATE TABLE employees_list_sales PARTITION OF employees_list FOR VALUES IN ('Sales');
CREATE TABLE employees_list_default PARTITION OF employees_list DEFAULT;

INSERT INTO employees_list (id, name, department) VALUES (1, 'Alice', 'Engineering');
INSERT INTO employees_list (id, name, department) VALUES (2, 'Bob', 'Sales');
INSERT INTO employees_list (id, name, department) VALUES (3, 'Charlie', 'Marketing');

CREATE TABLE orders_hash (
    order_id INT,
    customer_id INT,
    amount INT,
    PRIMARY KEY (order_id, customer_id)
) PARTITION BY HASH (customer_id);

CREATE TABLE orders_hash_p0 PARTITION OF orders_hash FOR VALUES WITH (MODULUS 3, REMAINDER 0);
CREATE TABLE orders_hash_p1 PARTITION OF orders_hash FOR VALUES WITH (MODULUS 3, REMAINDER 1);
CREATE TABLE orders_hash_p2 PARTITION OF orders_hash FOR VALUES WITH (MODULUS 3, REMAINDER 2);

INSERT INTO orders_hash (order_id, customer_id, amount) VALUES (1, 101, 500);
INSERT INTO orders_hash (order_id, customer_id, amount) VALUES (2, 102, 600);
INSERT INTO orders_hash (order_id, customer_id, amount) VALUES (3, 103, 700);
