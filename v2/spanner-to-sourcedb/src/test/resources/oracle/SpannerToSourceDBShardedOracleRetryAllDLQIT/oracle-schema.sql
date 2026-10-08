CREATE TABLE "Customers" (
    "CustomerId" NUMBER NOT NULL PRIMARY KEY,
    "CustomerName" VARCHAR2(255),
    "CreditLimit" NUMBER NOT NULL,
    "LegacyRegion" VARCHAR2(50),
    CONSTRAINT "CHK_CreditLimit" CHECK ("CreditLimit" > 1000)
);

CREATE TABLE "Orders" (
    "CustomerId" NUMBER NOT NULL,
    "OrderId" NUMBER NOT NULL,
    "OrderValue" NUMBER,
    "LegacyOrderSystem" VARCHAR2(50) NOT NULL,
    PRIMARY KEY ("CustomerId", "LegacyOrderSystem", "OrderId"),
    CONSTRAINT "FK_CustomerOrder" FOREIGN KEY ("CustomerId") REFERENCES "Customers"("CustomerId")
);

CREATE TABLE "AllDataTypes" (
    "id" NUMBER PRIMARY KEY,
    "varchar_col" VARCHAR2(1000) DEFAULT NULL,
    "bit8_col" RAW(8) DEFAULT NULL,
    "bit1_col" NUMBER(1) DEFAULT NULL,
    "boolean_col" NUMBER(1) DEFAULT NULL
);
