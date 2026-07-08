CREATE TYPE myenum AS ENUM ('enum1', 'enum2', 'enum3');

CREATE TABLE t_bigint (id serial primary key, col bigint);
CREATE TABLE t_bigserial (id serial primary key, col bigserial);

INSERT INTO t_bigint (col) VALUES (-9223372036854775808), (9223372036854775807), (42), (NULL);
INSERT INTO t_bigserial (col) VALUES (-9223372036854775808), (9223372036854775807), (42);

CREATE TABLE t_uuid_pk (id uuid PRIMARY KEY, col uuid NOT NULL);
INSERT INTO t_uuid_pk (id, col) VALUES
('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid),
('b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22'::uuid, 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22'::uuid);
