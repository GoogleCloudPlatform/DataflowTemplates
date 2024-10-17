# Collation outputs for unit test.
## Overview
As we can't run the entire collation query on derby during unit tests (due to variations in sql syntax and collation support), we have dumps of outputs of the collation query for a few cases to test in our unit tests.

| Dialect | Collation Name     | Resource File name                                                                                            | Remark                                                            |
|---------|--------------------|---------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------|
| Mysql   | utf8mb4_0900_ai_ci | [TestCollations/collation-output-mysql-utf8mb4-0900-ai-ci.tsv](collation-output-mysql-utf8mb4-0900-ai-ci.tsv) | Recommended case and accent insensitive collation (No Pad Space). |
| Mysql   | utf8mb4_0900_as_cs | [TestCollations/collation-output-mysql-utf8mb4-0900-as-cs.tsv](collation-output-mysql-utf8mb4-0900-as-cs.tsv) | Recommended case and accent sensitive collation (No Pad Space).   |
| Mysql   | utf8mb4_unicode_ci | [TestCollations/collation-output-mysql-utf8mb4-unicode-ci.tsv](collation-output-mysql-utf8mb4-unicode-ci.tsv) | Allows us test Pad Space collations.                              |

## How to generate a resource file for testing
You will need to run your query file like src/main/resources/sql/mysql_collation_oder_query.sql on a mysql instance and dump it's output to a file by following:

1. Replace the `charset_replacement_tag` and `collation_replacement_tag` with the required character-set and collation like:
```sql
SET @db_charset = '<character_set>';
SET @db_collation = '<collation>';  
```
Note it's recommended to make a copy of the sql file before editing to avoid un-intended or temporary changes in the script being pushed to a code review.

2. Run the edited copy of the sql file and dump the output to a tsv.
```bash
time mysql -u '<user_name>' -p'<password>' -h <ip> <db> <  mysql_collation_order_query.sql 2>&1 > /tmp/output.tsv 
```
3. Ensure that the dump has all the control characters escaped. Mysql does a good job at escaping most of the control characters when it dumps the output to a file, execept for \r which causes a line break.

4. Copy the output to the resources folder.

5. Add a test case that uses the new output in CollationMapperTest.java. While you will add the collation specific cases to this test (like say case sensitivity or pad space comparison etc), the CollationMapper class also runs checks during construction to ensure that the collation query output passes basic sanity checks (like no duplicate entries, an equivalent-character must map to itself, no holes in the index-ranking etc)

6. Update this [READMe](./README.md) as needed.