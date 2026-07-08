## S1L1 - shard1 - backfill
Insert into Users values(1, 'Tester1', 101); Insert into Users values(2, 'Tester2', 102); Insert into Users values(3, 'Tester3', 103);

## S1L2 - shard2 - backfill
INSERT INTO Users VALUES (4, 'Tester4', 104); INSERT INTO Users VALUES (5, 'Tester5', 105); INSERT INTO Users VALUES (6, 'Tester6', 106);

## S2L1 - shard3 - backfill
INSERT INTO Users VALUES (7, 'Tester7', 107); INSERT INTO Users VALUES (8, 'Tester8', 108); INSERT INTO Users VALUES (9, 'Tester9', 109);

## S2L2 - shard4 - backfill
INSERT INTO Users VALUES (10, 'Tester10', 110); INSERT INTO Users VALUES (11, 'Tester11', 111); INSERT INTO Users VALUES (12, 'Tester12', 112);

## S1L1 - shard1 - CDC
Update Users Set age=20 where id=1; Insert into Users values(13, 'Tester13', 113); delete from Users where id=2;