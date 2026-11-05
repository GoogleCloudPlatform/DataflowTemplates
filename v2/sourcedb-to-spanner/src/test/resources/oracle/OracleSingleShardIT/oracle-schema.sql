CREATE TABLE "SingleShardWithTransformationTable" (
  "pkid" INTEGER NOT NULL PRIMARY KEY,
  "name" VARCHAR2(20),
  "status" VARCHAR2(20)
);

INSERT INTO "SingleShardWithTransformationTable" ("pkid", "name", "status") VALUES (1, 'Alice', 'active');
INSERT INTO "SingleShardWithTransformationTable" ("pkid", "name", "status") VALUES (2, 'Bob', 'inactive');
INSERT INTO "SingleShardWithTransformationTable" ("pkid", "name", "status") VALUES (3, 'Carol', 'pending');
INSERT INTO "SingleShardWithTransformationTable" ("pkid", "name", "status") VALUES (4, 'David', 'complete');
INSERT INTO "SingleShardWithTransformationTable" ("pkid", "name", "status") VALUES (5, 'Emily', 'error');
