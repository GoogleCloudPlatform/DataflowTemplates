CREATE TABLE "Customers" (
    "CustomerId" INTEGER NOT NULL,
    "CustomerName" VARCHAR2(255),
    "CreditLimit" NUMBER NOT NULL,
    "LoyaltyTier" VARCHAR2(50),
    PRIMARY KEY ("CustomerId")
);

CREATE TABLE "Orders" (
    "CustomerId" INTEGER NOT NULL,
    "OrderId" INTEGER NOT NULL,
    "OrderValue" NUMBER,
    "OrderSource" VARCHAR2(50) NOT NULL,
    PRIMARY KEY ("CustomerId", "OrderId")
);

CREATE TABLE "AllDataTypes" (
    "id" INTEGER,
    "varchar2_col" VARCHAR2(1000),
    PRIMARY KEY ("id")
);
