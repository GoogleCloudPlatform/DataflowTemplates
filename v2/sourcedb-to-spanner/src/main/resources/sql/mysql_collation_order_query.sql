-- Returns all valid characters in the given charset with their WEIGHT_STRING sort keys.
-- All grouping, ranking and equivalent-char computation is done in Java (see CollationMapper).
-- No window functions are used, making this query fully compatible with MySQL 5.7+.
-- WEIGHT_STRING() has been available since MySQL 5.6.
--
-- charset_replacement_tag  is replaced with the actual charset  name by the Java adapter.
-- collation_replacement_tag is replaced with the actual collation name by the Java adapter.
--
-- weight_non_trailing: sort key for the character when surrounded by 'a' on both sides
--   (forces non-trailing evaluation, so PAD SPACE stripping does not affect this weight).
-- weight_trailing: sort key for the bare character (reflects PAD SPACE semantics for
--   collations that strip trailing spaces).
-- is_empty: 1 if the collation treats this character as invisible at all positions
--   (e.g. control characters such as U+001A in utf8mb4_0900_ai_ci).
-- is_space: 1 if the collation treats this character as equivalent to a trailing space.

SELECT
    charset_char,
    WEIGHT_STRING(
        CONCAT(
            CONVERT('a' USING charset_replacement_tag),
            charset_char,
            CONVERT('a' USING charset_replacement_tag)
        ) COLLATE collation_replacement_tag
    ) AS weight_non_trailing,
    WEIGHT_STRING(charset_char COLLATE collation_replacement_tag) AS weight_trailing,
    (
        CONCAT(
            CONVERT('a' USING charset_replacement_tag),
            charset_char,
            CONVERT('a' USING charset_replacement_tag)
        ) = CONCAT(
            CONVERT('a' USING charset_replacement_tag),
            CONVERT('a' USING charset_replacement_tag)
        ) COLLATE collation_replacement_tag
    ) AS is_empty,
    (
        CONCAT(
            CONVERT('a' USING charset_replacement_tag),
            charset_char,
            CONVERT('a' USING charset_replacement_tag)
        ) = CONCAT(
            CONVERT('a' USING charset_replacement_tag),
            CONVERT(' ' USING charset_replacement_tag),
            CONVERT('a' USING charset_replacement_tag)
        ) COLLATE collation_replacement_tag
    ) AS is_space
FROM (
    -- 3-byte codepoints: cross-join 6 hex nibbles to produce every 3-byte sequence.
    SELECT charset_char FROM (
        SELECT CONVERT(UNHEX(CONCAT(n1.n, n2.n, n3.n, n4.n, n5.n, n6.n)) USING charset_replacement_tag) AS charset_char
        FROM
            (SELECT '0' AS n UNION ALL SELECT '1' UNION ALL SELECT '2' UNION ALL SELECT '3' UNION ALL SELECT '4' UNION ALL SELECT '5' UNION ALL SELECT '6' UNION ALL SELECT '7' UNION ALL SELECT '8' UNION ALL SELECT '9' UNION ALL SELECT 'a' UNION ALL SELECT 'b' UNION ALL SELECT 'c' UNION ALL SELECT 'd' UNION ALL SELECT 'e' UNION ALL SELECT 'f') AS n1
            CROSS JOIN (SELECT '0' AS n UNION ALL SELECT '1' UNION ALL SELECT '2' UNION ALL SELECT '3' UNION ALL SELECT '4' UNION ALL SELECT '5' UNION ALL SELECT '6' UNION ALL SELECT '7' UNION ALL SELECT '8' UNION ALL SELECT '9' UNION ALL SELECT 'a' UNION ALL SELECT 'b' UNION ALL SELECT 'c' UNION ALL SELECT 'd' UNION ALL SELECT 'e' UNION ALL SELECT 'f') AS n2
            CROSS JOIN (SELECT '0' AS n UNION ALL SELECT '1' UNION ALL SELECT '2' UNION ALL SELECT '3' UNION ALL SELECT '4' UNION ALL SELECT '5' UNION ALL SELECT '6' UNION ALL SELECT '7' UNION ALL SELECT '8' UNION ALL SELECT '9' UNION ALL SELECT 'a' UNION ALL SELECT 'b' UNION ALL SELECT 'c' UNION ALL SELECT 'd' UNION ALL SELECT 'e' UNION ALL SELECT 'f') AS n3
            CROSS JOIN (SELECT '0' AS n UNION ALL SELECT '1' UNION ALL SELECT '2' UNION ALL SELECT '3' UNION ALL SELECT '4' UNION ALL SELECT '5' UNION ALL SELECT '6' UNION ALL SELECT '7' UNION ALL SELECT '8' UNION ALL SELECT '9' UNION ALL SELECT 'a' UNION ALL SELECT 'b' UNION ALL SELECT 'c' UNION ALL SELECT 'd' UNION ALL SELECT 'e' UNION ALL SELECT 'f') AS n4
            CROSS JOIN (SELECT '0' AS n UNION ALL SELECT '1' UNION ALL SELECT '2' UNION ALL SELECT '3' UNION ALL SELECT '4' UNION ALL SELECT '5' UNION ALL SELECT '6' UNION ALL SELECT '7' UNION ALL SELECT '8' UNION ALL SELECT '9' UNION ALL SELECT 'a' UNION ALL SELECT 'b' UNION ALL SELECT 'c' UNION ALL SELECT 'd' UNION ALL SELECT 'e' UNION ALL SELECT 'f') AS n5
            CROSS JOIN (SELECT '0' AS n UNION ALL SELECT '1' UNION ALL SELECT '2' UNION ALL SELECT '3' UNION ALL SELECT '4' UNION ALL SELECT '5' UNION ALL SELECT '6' UNION ALL SELECT '7' UNION ALL SELECT '8' UNION ALL SELECT '9' UNION ALL SELECT 'a' UNION ALL SELECT 'b' UNION ALL SELECT 'c' UNION ALL SELECT 'd' UNION ALL SELECT 'e' UNION ALL SELECT 'f') AS n6
    ) AS dt3
    WHERE CHAR_LENGTH(charset_char) <= 1 AND charset_char IS NOT NULL

    UNION ALL

    -- 2-byte codepoints: cross-join 4 hex nibbles to produce every 2-byte sequence.
    SELECT charset_char FROM (
        SELECT CONVERT(UNHEX(CONCAT(n1.n, n2.n, n3.n, n4.n)) USING charset_replacement_tag) AS charset_char
        FROM
            (SELECT '0' AS n UNION ALL SELECT '1' UNION ALL SELECT '2' UNION ALL SELECT '3' UNION ALL SELECT '4' UNION ALL SELECT '5' UNION ALL SELECT '6' UNION ALL SELECT '7' UNION ALL SELECT '8' UNION ALL SELECT '9' UNION ALL SELECT 'a' UNION ALL SELECT 'b' UNION ALL SELECT 'c' UNION ALL SELECT 'd' UNION ALL SELECT 'e' UNION ALL SELECT 'f') AS n1
            CROSS JOIN (SELECT '0' AS n UNION ALL SELECT '1' UNION ALL SELECT '2' UNION ALL SELECT '3' UNION ALL SELECT '4' UNION ALL SELECT '5' UNION ALL SELECT '6' UNION ALL SELECT '7' UNION ALL SELECT '8' UNION ALL SELECT '9' UNION ALL SELECT 'a' UNION ALL SELECT 'b' UNION ALL SELECT 'c' UNION ALL SELECT 'd' UNION ALL SELECT 'e' UNION ALL SELECT 'f') AS n2
            CROSS JOIN (SELECT '0' AS n UNION ALL SELECT '1' UNION ALL SELECT '2' UNION ALL SELECT '3' UNION ALL SELECT '4' UNION ALL SELECT '5' UNION ALL SELECT '6' UNION ALL SELECT '7' UNION ALL SELECT '8' UNION ALL SELECT '9' UNION ALL SELECT 'a' UNION ALL SELECT 'b' UNION ALL SELECT 'c' UNION ALL SELECT 'd' UNION ALL SELECT 'e' UNION ALL SELECT 'f') AS n3
            CROSS JOIN (SELECT '0' AS n UNION ALL SELECT '1' UNION ALL SELECT '2' UNION ALL SELECT '3' UNION ALL SELECT '4' UNION ALL SELECT '5' UNION ALL SELECT '6' UNION ALL SELECT '7' UNION ALL SELECT '8' UNION ALL SELECT '9' UNION ALL SELECT 'a' UNION ALL SELECT 'b' UNION ALL SELECT 'c' UNION ALL SELECT 'd' UNION ALL SELECT 'e' UNION ALL SELECT 'f') AS n4
    ) AS dt2
    WHERE CHAR_LENGTH(charset_char) <= 1 AND charset_char IS NOT NULL

    UNION ALL

    -- 1-byte codepoints: cross-join 2 hex nibbles to produce all 256 single-byte values.
    SELECT charset_char FROM (
        SELECT CONVERT(UNHEX(CONCAT(n1.n, n2.n)) USING charset_replacement_tag) AS charset_char
        FROM
            (SELECT '0' AS n UNION ALL SELECT '1' UNION ALL SELECT '2' UNION ALL SELECT '3' UNION ALL SELECT '4' UNION ALL SELECT '5' UNION ALL SELECT '6' UNION ALL SELECT '7' UNION ALL SELECT '8' UNION ALL SELECT '9' UNION ALL SELECT 'a' UNION ALL SELECT 'b' UNION ALL SELECT 'c' UNION ALL SELECT 'd' UNION ALL SELECT 'e' UNION ALL SELECT 'f') AS n1
            CROSS JOIN (SELECT '0' AS n UNION ALL SELECT '1' UNION ALL SELECT '2' UNION ALL SELECT '3' UNION ALL SELECT '4' UNION ALL SELECT '5' UNION ALL SELECT '6' UNION ALL SELECT '7' UNION ALL SELECT '8' UNION ALL SELECT '9' UNION ALL SELECT 'a' UNION ALL SELECT 'b' UNION ALL SELECT 'c' UNION ALL SELECT 'd' UNION ALL SELECT 'e' UNION ALL SELECT 'f') AS n2
    ) AS dt1
    WHERE CHAR_LENGTH(charset_char) <= 1 AND charset_char IS NOT NULL
) AS all_chars
WHERE charset_char IS NOT NULL
