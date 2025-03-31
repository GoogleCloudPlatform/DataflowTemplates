-- Top-level Parent table (valid).
CREATE TABLE Parent (
  ParentID INT64 NOT NULL,
  Value STRING(100)
) PRIMARY KEY (ParentID);

-- Depth 1: Child1 is interleaved in Parent.
CREATE TABLE Child1 (
  ParentID INT64 NOT NULL,
  Child1ID INT64 NOT NULL,
  Value STRING(100)
) PRIMARY KEY (ParentID, Child1ID),
  INTERLEAVE IN PARENT Parent ON DELETE CASCADE;

-- Depth 2: Child2 is interleaved in Child1.
CREATE TABLE Child2 (
  ParentID INT64 NOT NULL,
  Child1ID INT64 NOT NULL,
  Child2ID INT64 NOT NULL,
  Value STRING(100)
) PRIMARY KEY (ParentID, Child1ID, Child2ID),
  INTERLEAVE IN PARENT Child1 ON DELETE CASCADE;

-- Depth 3: Child3 is interleaved in Child2.
CREATE TABLE Child3 (
  ParentID INT64 NOT NULL,
  Child1ID INT64 NOT NULL,
  Child2ID INT64 NOT NULL,
  Child3ID INT64 NOT NULL,
  Value STRING(100)
) PRIMARY KEY (ParentID, Child1ID, Child2ID, Child3ID),
  INTERLEAVE IN PARENT Child2 ON DELETE CASCADE;

-- Depth 4: Child4 is interleaved in Child3.
CREATE TABLE Child4 (
  ParentID INT64 NOT NULL,
  Child1ID INT64 NOT NULL,
  Child2ID INT64 NOT NULL,
  Child3ID INT64 NOT NULL,
  Child4ID INT64 NOT NULL,
  Value STRING(100)
) PRIMARY KEY (ParentID, Child1ID, Child2ID, Child3ID, Child4ID),
  INTERLEAVE IN PARENT Child3 ON DELETE CASCADE;

-- Depth 5: Child5 is interleaved in Child4.
CREATE TABLE Child5 (
  ParentID INT64 NOT NULL,
  Child1ID INT64 NOT NULL,
  Child2ID INT64 NOT NULL,
  Child3ID INT64 NOT NULL,
  Child4ID INT64 NOT NULL,
  Child5ID INT64 NOT NULL,
  Value STRING(100)
) PRIMARY KEY (ParentID, Child1ID, Child2ID, Child3ID, Child4ID, Child5ID),
  INTERLEAVE IN PARENT Child4 ON DELETE CASCADE;

-- Depth 6: Child6 is interleaved in Child5.
CREATE TABLE Child6 (
  ParentID INT64 NOT NULL,
  Child1ID INT64 NOT NULL,
  Child2ID INT64 NOT NULL,
  Child3ID INT64 NOT NULL,
  Child4ID INT64 NOT NULL,
  Child5ID INT64 NOT NULL,
  Child6ID INT64 NOT NULL,
  Value STRING(100)
) PRIMARY KEY (ParentID, Child1ID, Child2ID, Child3ID, Child4ID, Child5ID, Child6ID),
  INTERLEAVE IN PARENT Child5 ON DELETE CASCADE;

-- Depth 7: Child7 is interleaved in Child6.
CREATE TABLE Child7 (
  ParentID INT64 NOT NULL,
  Child1ID INT64 NOT NULL,
  Child2ID INT64 NOT NULL,
  Child3ID INT64 NOT NULL,
  Child4ID INT64 NOT NULL,
  Child5ID INT64 NOT NULL,
  Child6ID INT64 NOT NULL,
  Child7ID INT64 NOT NULL,
  Value STRING(100)
) PRIMARY KEY (ParentID, Child1ID, Child2ID, Child3ID, Child4ID, Child5ID, Child6ID, Child7ID),
  INTERLEAVE IN PARENT Child6 ON DELETE CASCADE;

-- Depth 8: Child8 is interleaved in Child7.
-- This table exceeds the maximum allowed interleaving depth (8 > 7) and will trigger a failure.
CREATE TABLE Child8 (
  ParentID INT64 NOT NULL,
  Child1ID INT64 NOT NULL,
  Child2ID INT64 NOT NULL,
  Child3ID INT64 NOT NULL,
  Child4ID INT64 NOT NULL,
  Child5ID INT64 NOT NULL,
  Child6ID INT64 NOT NULL,
  Child7ID INT64 NOT NULL,
  Child8ID INT64 NOT NULL,
  Value STRING(100)
) PRIMARY KEY (ParentID, Child1ID, Child2ID, Child3ID, Child4ID, Child5ID, Child6ID, Child7ID, Child8ID),
  INTERLEAVE IN PARENT Child7 ON DELETE CASCADE;
