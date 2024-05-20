### CDC + Backfill for insert, delete and updates
# First wave of INSERT
Insert into Users values(1, 'Tester Kumar', 30, 0, 'A', '2023-01-01'); Insert into Users values(2, 'Tester Yadav', 33, 1, 'B', '2024-01-01');

# Second wave of INSERT, UPDATE, DELETE
Delete from Users where id=2; Insert into Users values(4, 'Tester', 38, 1, 'D', '2023-09-10'); Insert into Users values(3, 'Tester Gupta', 36, 0, 'C', '2023-06-07'); Update Users set age=50, plan='Z' where id=3;

### Insert only
# First wave of INSERT
Insert into Movie values(1, 'movie1', 12345.09876, '2023-01-01 12:12:12'); Insert into Movie values(2, 'movie2', 931.5123, '2023-11-25 17:10:12');

### Foreign key constraint test
Insert into Authors values(1, 'a1'); Insert into Authors values(2, 'a2'); Insert into Authors values(3, 'a3'); insert into Authors values(4, 'a4');
Insert into Articles values(1, 'Article001', '2024-01-01', 1); Insert into Articles values(2, 'Article002', '2024-01-01', 1); Insert into Articles values(3, 'Article004', '2024-01-01', 4); Insert into Articles values(4, 'Article005', '2024-01-01', 3);
Insert into Books values(1, 'Book005', 3); Insert into Books values(2, 'Book002', 3); Insert into Books values(3, 'Book004', 4); Insert into Books values(4, 'Book005', 2);
