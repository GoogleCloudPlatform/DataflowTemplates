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

## S1L2 - shard2 - CDC
Update Users Set age=21 where id=4; Insert into Users values(14, 'Tester14', 114); delete from Users where id=5;

## S2L1 - shard3 - CDC
Update Users Set age=22 where id=7; Insert into Users values(15, 'Tester15', 115); delete from Users where id=8;

## S2L2 - shard4 - CDC
Update Users Set age=23 where id=10; Insert into Users values(16, 'Tester16', 116); delete from Users where id=11;


## PK reordered test

## S1L1 - shard1
Insert into Movie values(1, 1, 'Mov123', 3); Insert into Movie values(6, 8, 'Mov973', 15); Insert into Movie values(26, 3, 'Mov637', 3);
Update Movie Set actor=27 where id1=1 AND id2=1; Insert into Movie values(18, 12, 'Tester363', 40); delete from Movie where id1=6 AND id2=8;

## S1L2 - shard2
Insert into Movie values(18, 23, 'Mov945', 18); Insert into Movie values(26, 12, 'Mov764', 9); Insert into Movie values(17, 27, 'Mov294', 25);
Update Movie Set actor=8 where id1=26 AND id2=12; Insert into Movie values(13, 8, 'Tester828', 15); delete from Movie where id1=17 AND id2=27;


## Custom transformation test

## S1L1 - shard1
Insert into Customers values(1, 'first1', 'last1'); Insert into Customers values(2, 'first2', 'last2');

## S1L2 - shard2
Insert into Customers values(1, 'first1', 'last1'); Insert into Customers values(2, 'first2', 'last2');