-- Converts codepoint to the given charset safely. Instead of throwing errors, NULLs are returned instead.
-- The return type is defined based on PAD_SPACE. In PostgreSQL fixed length character types (CHAR(X), BPCHAR(X)),
-- pad trailing space, while variable length character types (CHAR (no length), BPCHAR (no length), VARCHAR and TEXT)
-- do not.
CREATE OR REPLACE FUNCTION pg_temp.safe_convert_from(codepoint int8, charset text) RETURNS return_type_replacement_tag AS
'
  BEGIN
    RETURN convert_from(decode(to_hex(codepoint), ''hex''), charset);
  EXCEPTION
    WHEN OTHERS THEN RETURN NULL;
  END;
'
LANGUAGE plpgsql;

WITH

-- Generate 1 byte (U+0000 to U+007F), 2 bytes (U+0080 to U+07FF),
-- 3 bytes (U+0800 to U+FFFF), and 4 bytes (U+10000 to U+10FFFF) unicode
-- codepoints
charset_chars AS (
  SELECT *
  FROM (
    SELECT
      generate_series(0, 1114111),
      pg_temp.safe_convert_from(generate_series(0, 1114111), 'charset_replacement_tag')
  ) dt(codepoint, charset_char)
  WHERE LENGTH(charset_char) <= 1 AND charset_char IS NOT NULL
),

-- Compute is_empty and is_space flags.
-- equivalent_charset_char and codepoint_rank are computed in Java (CollationMapper)
-- from the DENSE_RANK values returned here, removing the need for the FIRST_VALUE
-- window-function CTEs that were previously required.
charset_chars_with_flags AS (
  SELECT
    codepoint,
    charset_char,
    CONCAT('a', charset_char, 'a') = 'aa' COLLATE "collation_replacement_tag" AS is_empty,
    CONCAT('a', charset_char, 'a') = 'a a' COLLATE "collation_replacement_tag" AS is_space
  FROM charset_chars
),

-- Assign dense ranks per collation order.
-- Java uses these ranks to determine equivalent characters (min codepoint per rank group)
-- without needing the database to compute FIRST_VALUE over collation-partitioned windows.
ranked AS (
  SELECT
    *,
    DENSE_RANK() OVER (
      PARTITION BY is_empty
      ORDER BY (CONCAT('a', charset_char, 'a') COLLATE "collation_replacement_tag")
    ) - 1 AS codepoint_rank,
    DENSE_RANK() OVER (
      PARTITION BY is_empty, is_space
      ORDER BY (charset_char COLLATE "collation_replacement_tag")
    ) - 1 AS codepoint_rank_pad_space
  FROM charset_chars_with_flags
  ORDER BY codepoint ASC
)

SELECT *
FROM ranked
ORDER BY codepoint ASC;
