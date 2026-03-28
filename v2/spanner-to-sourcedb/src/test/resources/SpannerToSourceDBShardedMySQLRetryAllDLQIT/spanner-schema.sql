CREATE TABLE Customers (
    CustomerId INT64 NOT NULL,
    CustomerName STRING(255),
    CreditLimit NUMERIC,         -- No constraint
    LoyaltyTier STRING(50),      -- Renamed from LegacyRegion of MySQL
) PRIMARY KEY (CustomerId);

CREATE TABLE Orders (
    CustomerId INT64 NOT NULL,
    OrderId INT64 NOT NULL,
    OrderValue NUMERIC,
    OrderSource STRING(50) NOT NULL, -- Added column, NOT part of Spanner PK
) PRIMARY KEY (CustomerId, OrderId);

CREATE TABLE AllDataTypes (
	id INT64 NOT NULL ,             -- From: id int(10)
	varchar_col STRING(1000),       -- From: varchar_col varchar(1000)
	tinyint_col INT64,              -- From: tinyint_col tinyint(3)
	tinyint_unsigned_col INT64,     -- From: tinyint_unsigned_col tinyint
	text_col STRING(MAX),           -- From: text_col text(65535)
	date_col DATE,                  -- From: date_col date
	smallint_col INT64,             -- From: smallint_col smallint(5)
	smallint_unsigned_col INT64,    -- From: smallint_unsigned_col smallint(5)
	mediumint_col INT64,            -- From: mediumint_col mediumint(7)
	mediumint_unsigned_col INT64,   -- From: mediumint_unsigned_col mediumint(7)
	bigint_col INT64,               -- From: bigint_col bigint(19)
	bigint_unsigned_col INT64,      -- From: bigint_unsigned_col bigint unsigned(20)
	float_col FLOAT32,              -- From: float_col float(12)
	double_col FLOAT64,             -- From: double_col double(22)
	decimal_col NUMERIC,            -- From: decimal_col decimal(65,30)
	datetime_col TIMESTAMP,         -- From: datetime_col datetime
	time_col STRING(MAX),           -- From: time_col time
	year_col STRING(MAX),           -- From: year_col year
	char_col STRING(255),           -- From: char_col char(255)
	tinyblob_col BYTES(255),        -- From: tinyblob_col tinyblob(255)
	tinytext_col STRING(MAX),       -- From: tinytext_col tinytext(255)
	blob_col BYTES(65535),          -- From: blob_col blob(65535)
	mediumblob_col BYTES(10485760), -- From: mediumblob_col mediumblob(16777215)
	mediumtext_col STRING(MAX),     -- From: mediumtext_col mediumtext(16777215)
	test_json_col JSON,             -- From: test_json_col json
	longblob_col BYTES(10485760),   -- From: longblob_col longblob(4294967295)
	longtext_col STRING(MAX),       -- From: longtext_col longtext(4294967295)
	enum_col STRING(MAX),           -- From: enum_col enum(1)
	bool_col BOOL,                  -- From: bool_col tinyint(1)
	binary_col BYTES(255),          -- From: binary_col binary(255)
	varbinary_col BYTES(1000),      -- From: varbinary_col varbinary(1000)
	bit_col BYTES(MAX),             -- From: bit_col bit(64)
	bit8_col BYTES(MAX),            -- From: bit8_col bit(8)
	bit1_col BOOL,                  -- From: bit1_col bit(1)
	boolean_col BOOL,               -- From: boolean_col tinyint(1)
	int_col INT64,                  -- From: int_col int(10)
	integer_unsigned_col INT64,     -- From: integer_unsigned_col int(10)
	timestamp_col TIMESTAMP,        -- From: timestamp_col timestamp
	set_col STRING(MAX),            -- From: set_col set[]
) PRIMARY KEY (id);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d',
  allow_txn_exclusion = true
);
