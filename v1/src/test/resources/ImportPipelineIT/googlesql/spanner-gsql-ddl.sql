DROP TABLE IF EXISTS EmptyTable;
CREATE TABLE EmptyTable (
                            id INT64 NOT NULL,
) PRIMARY KEY(id);

DROP TABLE IF EXISTS Singers;
CREATE TABLE Singers (
                         Id INT64,
                         FirstName STRING(MAX),
                         LastName STRING(MAX),
                         Review STRING(MAX),
                         MyTokens TOKENLIST AS (TOKENIZE_FULLTEXT(Review)) HIDDEN,
) PRIMARY KEY(Id);

DROP TABLE IF EXISTS Float32Table;
CREATE TABLE Float32Table (
                              Key STRING(MAX) NOT NULL,
                              Float32Value FLOAT32,
) PRIMARY KEY(Key);

DROP TABLE IF EXISTS CustomDictionary;
CREATE TABLE CustomDictionary (
    Key STRING(MAX) NOT NULL,
    Value ARRAY<STRING(MAX)> NOT NULL,
) PRIMARY KEY(Key ASC), OPTIONS (fulltext_dictionary_table=true);
