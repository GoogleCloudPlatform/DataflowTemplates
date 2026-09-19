CREATE TABLE Customers (
    CustomerId INT64 NOT NULL,
    CustomerName STRING(255),
    CreditLimit NUMERIC,
    LegacyRegion STRING(50),
) PRIMARY KEY (CustomerId);

ALTER TABLE Customers ADD CONSTRAINT CHK_CreditLimit CHECK (CreditLimit > 1000);

CREATE TABLE Orders (
    CustomerId INT64 NOT NULL,
    OrderId INT64 NOT NULL,
    OrderValue NUMERIC,
    LegacyOrderSystem STRING(50) NOT NULL,
) PRIMARY KEY (CustomerId, LegacyOrderSystem, OrderId);

ALTER TABLE Orders ADD CONSTRAINT FK_CustomerOrder FOREIGN KEY (CustomerId) REFERENCES Customers(CustomerId);

CREATE TABLE AllDataTypes (
    id INT64 NOT NULL,
    varchar2_col STRING(MAX),
) PRIMARY KEY(id);
