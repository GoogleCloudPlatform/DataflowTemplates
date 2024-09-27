-- Converts codepoint to the given charset safely. Instead of throwing errors, NULLs are returned instead.
CREATE OR REPLACE FUNCTION pg_temp.safe_convert_from(codepoint int8, charset text) RETURNS TEXT AS
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

-- It is possible to have equivalent characters in PostgreSQL under certain collations.
-- For example, ICU collations (https://www.postgresql.org/docs/current/collation.html#ICU-CUSTOM-COLLATIONS)
-- allow for ignoring case sensitivity, spaces and punctuation.
charset_chars_with_codepoints AS (
  SELECT
    codepoint,
    charset_char,
    CONCAT('a', charset_char, 'a') AS charset_char_non_trailing,
    CONCAT('a', charset_char, 'a') = 'aa' COLLATE "collation_replacement_tag" AS is_empty,
    CONCAT('a', charset_char, 'a') = 'a a' COLLATE "collation_replacement_tag" AS is_space
  FROM charset_chars
),

find_equivalents_query AS (
  SELECT
    *,
    FIRST_VALUE(codepoint) OVER (PARTITION BY (charset_char COLLATE "collation_replacement_tag"), is_empty ORDER BY (charset_char COLLATE "collation_replacement_tag"), codepoint) AS equivalent_codepoint,
    FIRST_VALUE(charset_char) OVER (PARTITION BY (charset_char COLLATE "collation_replacement_tag"), is_empty ORDER BY (charset_char COLLATE "collation_replacement_tag"), codepoint) AS equivalent_charset_char,
    FIRST_VALUE(codepoint) OVER (PARTITION BY (charset_char_non_trailing COLLATE "collation_replacement_tag"), is_empty ORDER BY (charset_char COLLATE "collation_replacement_tag"), codepoint) AS equivalent_codepoint_non_trailing,
    FIRST_VALUE(charset_char) OVER (PARTITION BY (charset_char_non_trailing COLLATE "collation_replacement_tag"), is_empty ORDER BY (charset_char COLLATE "collation_replacement_tag"), codepoint) AS equivalent_charset_char_non_trailing,
    FIRST_VALUE(codepoint) OVER (PARTITION BY (charset_char COLLATE "collation_replacement_tag"), is_empty, is_space ORDER BY (charset_char COLLATE "collation_replacement_tag"), codepoint) AS equivalent_codepoint_pad_space,
    FIRST_VALUE(charset_char) OVER (PARTITION BY (charset_char COLLATE "collation_replacement_tag"), is_empty, is_space ORDER BY (charset_char COLLATE "collation_replacement_tag"), codepoint) AS equivalent_charset_char_pad_space
  FROM charset_chars_with_codepoints
),

find_rank_query AS (
  SELECT
    *,
    DENSE_RANK() OVER (PARTITION BY is_empty ORDER BY (equivalent_charset_char_non_trailing COLLATE "collation_replacement_tag")) -1 AS codepoint_rank,
    DENSE_RANK() OVER (PARTITION BY is_empty, is_space ORDER BY (equivalent_charset_char_pad_space COLLATE "collation_replacement_tag")) -1 AS codepoint_rank_pad_space
  FROM find_equivalents_query
  ORDER BY codepoint ASC
)

SELECT *
FROM find_rank_query
ORDER BY codepoint ASC;
