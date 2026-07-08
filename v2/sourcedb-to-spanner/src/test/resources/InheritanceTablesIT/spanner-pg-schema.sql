CREATE TABLE vehicles (
    id bigint PRIMARY KEY,
    make character varying
);

CREATE TABLE cars (
    id bigint PRIMARY KEY,
    make character varying,
    doors bigint
);

CREATE TABLE sports_cars (
    id bigint PRIMARY KEY,
    make character varying,
    doors bigint,
    top_speed bigint
);
