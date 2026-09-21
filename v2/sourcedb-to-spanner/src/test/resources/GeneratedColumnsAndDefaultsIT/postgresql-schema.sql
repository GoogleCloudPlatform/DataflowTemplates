CREATE TABLE products (
    id        bigint      NOT NULL PRIMARY KEY,
    price     integer     NOT NULL,
    qty       integer     NOT NULL,
    total     integer     GENERATED ALWAYS AS (price * qty) STORED,
    sku       varchar(50) NOT NULL,
    sku_upper varchar(50) GENERATED ALWAYS AS (upper(sku)) STORED
);

INSERT INTO products (id, price, qty, sku) VALUES
    (1, 10, 2, 'abc'),
    (2, 5, 6, 'xyz'),
    (3, 100, 1, 'p-3');

CREATE TABLE gc_nullable (
    id     bigint  NOT NULL PRIMARY KEY,
    x      integer,
    y      integer,
    sum_xy integer GENERATED ALWAYS AS (x + y) STORED
);

INSERT INTO gc_nullable (id, x, y) VALUES
    (1, 3, 4),
    (2, NULL, 5);

CREATE TABLE gc_pk (
    a   integer     NOT NULL,
    tag varchar(10) NOT NULL,
    k   integer     GENERATED ALWAYS AS (a + 1) STORED,
    PRIMARY KEY (k, tag)
);

INSERT INTO gc_pk (a, tag) VALUES
    (1, 'x'),
    (10, 'y');

CREATE TABLE defaults_all (
    id       bigint      NOT NULL PRIMARY KEY,
    payload  text        NOT NULL,
    d_int    integer     DEFAULT 42,
    d_bigint bigint      DEFAULT 9000000000,
    d_str    varchar(20) DEFAULT 'NEW',
    d_bool   boolean     DEFAULT true,
    d_neg    integer     DEFAULT -7
);

INSERT INTO defaults_all (id, payload) VALUES
    (1, 'row-one'),
    (2, 'row-two');

INSERT INTO defaults_all (id, payload, d_int, d_bigint, d_str, d_bool, d_neg) VALUES
    (3, 'row-three', 1, 2, 'OLD', false, 3);

INSERT INTO defaults_all (id, payload, d_int, d_bigint, d_str, d_bool, d_neg) VALUES
    (4, 'row-four', NULL, NULL, NULL, NULL, NULL);

CREATE TABLE degraded_gencol (
    id    bigint  NOT NULL PRIMARY KEY,
    a     integer NOT NULL,
    b     integer NOT NULL,
    label text    GENERATED ALWAYS AS (a::text || '-' || b::text) STORED
);

INSERT INTO degraded_gencol (id, a, b) VALUES
    (1, 2, 3),
    (2, 10, 20);

CREATE TABLE gc_virtual (
    id      bigint      NOT NULL PRIMARY KEY,
    a       integer     NOT NULL,
    tag     varchar(20) NOT NULL,
    doubled integer     GENERATED ALWAYS AS (a * 2) STORED
);

INSERT INTO gc_virtual (id, a, tag) VALUES
    (1, 4, 'four'),
    (2, 25, 'twentyfive');
