CREATE TABLE `Customers` (
    `CustomerId` INT64 NOT NULL,
    `CustomerName` STRING(255),
    `CreditLimit` NUMERIC,
    `LoyaltyTier` STRING(50),
) PRIMARY KEY (`CustomerId`);

CREATE TABLE `Orders` (
    `CustomerId` INT64 NOT NULL,
    `OrderId` INT64 NOT NULL,
    `OrderValue` NUMERIC,
    `OrderSource` STRING(50) NOT NULL,
) PRIMARY KEY (`CustomerId`, `OrderId`);

CREATE TABLE `AllDataTypes` (
    `id` INT64 NOT NULL,
    `varchar_col` STRING(1000),
    `bit8_col` BYTES(MAX),
    `bit1_col` BOOL,
    `boolean_col` BOOL,
) PRIMARY KEY (`id`);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d',
  allow_txn_exclusion = true
);
