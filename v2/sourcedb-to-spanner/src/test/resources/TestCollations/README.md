# Collation Testing Strategy

## Overview
Unit tests in `CollationMapperTest.java` use in-memory synthetic character fixtures (~60 representative codepoints) covering:
- Standard ASCII letters and digits
- Accented characters (e.g. `á`, `ä`)
- Midpoint target characters (e.g. Latin Gamma `Ɣ` for `utf8mb4_0900_ai_ci`)
- Whitespace, non-breaking space, tabs, and control-Z empty characters
- Pad-space and non-pad-space collation rankings

For full end-to-end testing against real database engines, live queries (`src/main/resources/sql/mysql_collation_order_query.sql`) are executed against live MySQL 5.7 and MySQL 8.0 instances using Testcontainers in `MysqlCollationMapperIT.java`.