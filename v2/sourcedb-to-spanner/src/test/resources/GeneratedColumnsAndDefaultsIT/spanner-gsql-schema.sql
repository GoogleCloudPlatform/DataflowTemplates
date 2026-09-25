-- Mirrors the DDL that Spanner Migration Tool emits for the PostgreSQL source in
-- postgresql-schema.sql: STORED generated columns stay generated, column DEFAULTs
-- are carried over, and a generated column whose expression Spanner cannot accept
-- is degraded to a plain column.
CREATE TABLE products (
    id        INT64      NOT NULL,
    price     INT64      NOT NULL,
    qty       INT64      NOT NULL,
    total     INT64      AS (price * qty) STORED,
    sku       STRING(50) NOT NULL,
    sku_upper STRING(50) AS (UPPER(sku)) STORED,
) PRIMARY KEY (id);

-- Nullable inputs: Spanner must evaluate the expression, yielding NULL, rather
-- than the pipeline carrying the source value over.
CREATE TABLE gc_nullable (
    id     INT64 NOT NULL,
    x      INT64,
    y      INT64,
    sum_xy INT64 AS (x + y) STORED,
) PRIMARY KEY (id);

-- Generated column in the primary key. The mutation cannot supply k, so the key
-- only resolves if Spanner computes it.
CREATE TABLE gc_pk (
    a   INT64      NOT NULL,
    tag STRING(10) NOT NULL,
    k   INT64      NOT NULL AS (a + 1) STORED,
) PRIMARY KEY (k, tag);

-- d_spanner_only and d_spanner_notnull have no source counterpart and must be
-- omitted from the mutation so Spanner applies their defaults. d_spanner_notnull
-- is NOT NULL, so writing an explicit NULL instead would fail the row.
-- d_derived is generated from a defaulted column.
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

-- doubled is generated but NOT STORED. The pipeline decides what to skip from
-- IS_GENERATED in Spanner's information schema, which does not distinguish
-- stored from non-stored, so this must behave exactly like the STORED cases.
CREATE TABLE gc_virtual (
    id      INT64      NOT NULL,
    a       INT64      NOT NULL,
    tag     STRING(20) NOT NULL,
    doubled INT64      AS (a * 2),
) PRIMARY KEY (id);
