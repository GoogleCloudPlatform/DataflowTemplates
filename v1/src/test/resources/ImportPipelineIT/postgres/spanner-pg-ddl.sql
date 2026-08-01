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

DROP TABLE IF EXISTS "CustomDictionary";
CREATE TABLE "CustomDictionary" (
    "Key" character varying NOT NULL,
    "Value" character varying[] NOT NULL,
    PRIMARY KEY("Key")
) WITH (fulltext_dictionary_table=true);
