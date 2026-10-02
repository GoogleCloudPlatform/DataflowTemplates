-- The DDL Spanner Migration Tool generates for postgresql-schema.sql.
CREATE TABLE products (
    id        INT64      NOT NULL,
    price     INT64      NOT NULL,
    qty       INT64      NOT NULL,
    total     INT64      AS (price * qty) STORED,
    sku       STRING(50) NOT NULL,
    sku_upper STRING(50) AS (UPPER(sku)) STORED,
) PRIMARY KEY (id);

-- sum_xy is NULL when an input is NULL.
CREATE TABLE gc_nullable (
    id     INT64 NOT NULL,
    x      INT64,
    y      INT64,
    sum_xy INT64 AS (x + y) STORED,
) PRIMARY KEY (id);

-- k is generated and part of the primary key.
CREATE TABLE gc_pk (
    a   INT64      NOT NULL,
    tag STRING(10) NOT NULL,
    k   INT64      NOT NULL AS (a + 1) STORED,
) PRIMARY KEY (k, tag);

-- d_spanner_* exist only in Spanner, so Spanner fills them from their DEFAULT.
-- d_spanner_notnull fails the row if the pipeline writes NULL to it.
-- d_derived is generated from d_spanner_notnull.
CREATE TABLE defaults_all (
    id                INT64       NOT NULL,
    payload           STRING(MAX) NOT NULL,
    d_int             INT64       DEFAULT (42),
    d_bigint          INT64       DEFAULT (9000000000),
    d_str             STRING(20)  DEFAULT ('NEW'),
    d_bool            BOOL        DEFAULT (true),
    d_neg             INT64       DEFAULT (-7),
    d_spanner_only    INT64       DEFAULT (777),
    d_spanner_notnull INT64       NOT NULL DEFAULT (555),
    d_derived         INT64       AS (d_spanner_notnull * 2) STORED,
) PRIMARY KEY (id);

CREATE TABLE degraded_gencol (
    id    INT64       NOT NULL,
    a     INT64       NOT NULL,
    b     INT64       NOT NULL,
    label STRING(MAX),
) PRIMARY KEY (id);

-- doubled is a non-stored generated column.
CREATE TABLE gc_virtual (
    id      INT64      NOT NULL,
    a       INT64      NOT NULL,
    tag     STRING(20) NOT NULL,
    doubled INT64      AS (a * 2),
) PRIMARY KEY (id);
