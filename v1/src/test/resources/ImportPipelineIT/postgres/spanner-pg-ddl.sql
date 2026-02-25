DROP TABLE IF EXISTS "EmptyTable";
CREATE TABLE "EmptyTable" (
                              id bigint NOT NULL,
                              PRIMARY KEY(id)
);

DROP TABLE IF EXISTS "Singers";
CREATE TABLE "Singers" (
                           "Id" bigint,
                           "FirstName" character varying(256),
                           "LastName" character varying(256),
                           PRIMARY KEY("Id")
);

DROP TABLE IF EXISTS "Float32Table";
CREATE TABLE "Float32Table" (
                                "Key" character varying NOT NULL,
                                "Float32Value" real,
                                PRIMARY KEY("Key")
);
