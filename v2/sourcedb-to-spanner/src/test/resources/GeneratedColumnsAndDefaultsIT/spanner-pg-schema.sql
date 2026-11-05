-- PostgreSQL-dialect version of spanner-gsql-schema.sql. degraded_gencol.label
-- stays generated here because this dialect accepts :: casts.
CREATE TABLE products (
    id        bigint      NOT NULL,
    price     bigint      NOT NULL,
    qty       bigint      NOT NULL,
    total     bigint      GENERATED ALWAYS AS (price * qty) STORED,
    sku       varchar(50) NOT NULL,
    sku_upper varchar(50) GENERATED ALWAYS AS (upper(sku)) STORED,
    PRIMARY KEY (id)
);

CREATE TABLE gc_nullable (
    id     bigint NOT NULL,
    x      bigint,
    y      bigint,
    sum_xy bigint GENERATED ALWAYS AS (x + y) STORED,
    PRIMARY KEY (id)
);

CREATE TABLE gc_pk (
    a   bigint      NOT NULL,
    tag varchar(10) NOT NULL,
    k   bigint      NOT NULL GENERATED ALWAYS AS (a + 1) STORED,
    PRIMARY KEY (k, tag)
);

CREATE TABLE defaults_all (
    id                bigint      NOT NULL,
    payload           text        NOT NULL,
    d_int             bigint      DEFAULT 42,
    d_bigint          bigint      DEFAULT 9000000000,
    d_str             varchar(20) DEFAULT 'NEW',
    d_bool            boolean     DEFAULT true,
    d_neg             bigint      DEFAULT -7,
    d_spanner_only    bigint      DEFAULT 777,
    d_spanner_notnull bigint      NOT NULL DEFAULT 555,
    d_derived         bigint      GENERATED ALWAYS AS (d_spanner_notnull * 2) STORED,
    PRIMARY KEY (id)
);

CREATE TABLE degraded_gencol (
    id    bigint  NOT NULL,
    a     bigint  NOT NULL,
    b     bigint  NOT NULL,
    label text    GENERATED ALWAYS AS (a::text || '-' || b::text) STORED,
    PRIMARY KEY (id)
);

-- doubled is a non-stored generated column (VIRTUAL in this dialect).
CREATE TABLE gc_virtual (
    id      bigint      NOT NULL,
    a       bigint      NOT NULL,
    tag     varchar(20) NOT NULL,
    doubled bigint      GENERATED ALWAYS AS (a * 2) VIRTUAL,
    PRIMARY KEY (id)
);

CREATE TABLE plain_to_gc (
    id         bigint      NOT NULL,
    first_name varchar(20) NOT NULL,
    last_name  varchar(20) NOT NULL,
    full_name  varchar(41) GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
    PRIMARY KEY (id)
);
