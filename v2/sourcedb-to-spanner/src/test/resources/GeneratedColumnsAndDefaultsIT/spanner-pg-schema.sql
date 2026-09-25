-- Spanner PostgreSQL dialect counterpart of spanner-gsql-schema.sql. The dialect
-- accepts "::" casts, so Spanner Migration Tool keeps degraded_gencol.label
-- generated here instead of degrading it to a plain column.
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

-- Non-stored generated column. The PostgreSQL dialect requires the VIRTUAL
-- keyword, which is what Spanner Migration Tool emits for a PG 18 source column
-- declared GENERATED ALWAYS AS (...) VIRTUAL.
CREATE TABLE gc_virtual (
    id      bigint      NOT NULL,
    a       bigint      NOT NULL,
    tag     varchar(20) NOT NULL,
    doubled bigint      GENERATED ALWAYS AS (a * 2) VIRTUAL,
    PRIMARY KEY (id)
);
