### CDC + Backfill for insert, delete and updates
# First wave of INSERT
Insert into Category values(1, 'xyz', '2024-02-06T12:19:37'); Insert into Category values(2, 'abc', '2024-02-06T12:19:47');

# Second wave of INSERT, UPDATE, DELETE
Delete from Category where category_id=1; Insert into Category values(4, 'ghi', '2024-02-07T08:12:32'); Insert into Category values(3, 'def', '2024-02-07T08:10:35'); Update Category set name='abc1' where category_id=2;