-- In this query, at a high level we sort a sequence of variable length code points as per the collation order of the database to understand how the code points for characters compare for the given collation.
-- We start with all possible variable length code points which represent a single characters, sort them by the collation order and condense the output to groups.
-- More details are explained in the query that follows.
-- Note that although the query appears big it completes in a time < 60 seconds as the db just has to do in-memory sorting of literals.
-- TODO(vardhanvthigle): Currently this query might not be compatible with MySQl 5.7. Change window functions with inner joins.

-- Unfortunately we can't use prepared statement to set variable values via jdbc.
-- The dataflow code will replace the below tags with the actual db charset and collation.
SET @db_charset = 'charset_replacement_tag';
SET @db_collation = 'collation_replacement_tag';



-- Enumerating valid utf8mb4 byte sequences
SET @all_chars = '
SELECT CONCAT(n1.n, n2.n, n3.n, n4.n, n5.n, n6.n, n7.n, n8.n) AS hex_val
FROM (SELECT ''F'' AS n) n1
CROSS JOIN (SELECT ''0'' AS n UNION ALL SELECT ''1'' UNION ALL SELECT ''2'' UNION ALL SELECT ''3'' UNION ALL SELECT ''4'') n2
CROSS JOIN (SELECT ''8'' AS n UNION ALL SELECT ''9'' UNION ALL SELECT ''A'' UNION ALL SELECT ''B'') n3
CROSS JOIN (SELECT ''0'' AS n UNION ALL SELECT ''1'' UNION ALL SELECT ''2'' UNION ALL SELECT ''3'' UNION ALL SELECT ''4'' UNION ALL SELECT ''5'' UNION ALL SELECT ''6'' UNION ALL SELECT ''7'' UNION ALL SELECT ''8'' UNION ALL SELECT ''9'' UNION ALL SELECT ''A'' UNION ALL SELECT ''B'' UNION ALL SELECT ''C'' UNION ALL SELECT ''D'' UNION ALL SELECT ''E'' UNION ALL SELECT ''F'') n4
CROSS JOIN (SELECT ''8'' AS n UNION ALL SELECT ''9'' UNION ALL SELECT ''A'' UNION ALL SELECT ''B'') n5
CROSS JOIN (SELECT ''0'' AS n UNION ALL SELECT ''1'' UNION ALL SELECT ''2'' UNION ALL SELECT ''3'' UNION ALL SELECT ''4'' UNION ALL SELECT ''5'' UNION ALL SELECT ''6'' UNION ALL SELECT ''7'' UNION ALL SELECT ''8'' UNION ALL SELECT ''9'' UNION ALL SELECT ''A'' UNION ALL SELECT ''B'' UNION ALL SELECT ''C'' UNION ALL SELECT ''D'' UNION ALL SELECT ''E'' UNION ALL SELECT ''F'') n6
CROSS JOIN (SELECT ''8'' AS n UNION ALL SELECT ''9'' UNION ALL SELECT ''A'' UNION ALL SELECT ''B'') n7
CROSS JOIN (SELECT ''0'' AS n UNION ALL SELECT ''1'' UNION ALL SELECT ''2'' UNION ALL SELECT ''3'' UNION ALL SELECT ''4'' UNION ALL SELECT ''5'' UNION ALL SELECT ''6'' UNION ALL SELECT ''7'' UNION ALL SELECT ''8'' UNION ALL SELECT ''9'' UNION ALL SELECT ''A'' UNION ALL SELECT ''B'' UNION ALL SELECT ''C'' UNION ALL SELECT ''D'' UNION ALL SELECT ''E'' UNION ALL SELECT ''F'') n8
UNION ALL
SELECT CONCAT(n1.n, n2.n, n3.n, n4.n, n5.n, n6.n) AS hex_val
FROM (SELECT ''E'' AS n) n1
CROSS JOIN (SELECT ''0'' AS n UNION ALL SELECT ''1'' UNION ALL SELECT ''2'' UNION ALL SELECT ''3'' UNION ALL SELECT ''4'' UNION ALL SELECT ''5'' UNION ALL SELECT ''6'' UNION ALL SELECT ''7'' UNION ALL SELECT ''8'' UNION ALL SELECT ''9'' UNION ALL SELECT ''A'' UNION ALL SELECT ''B'' UNION ALL SELECT ''C'' UNION ALL SELECT ''D'' UNION ALL SELECT ''E'' UNION ALL SELECT ''F'') n2
CROSS JOIN (SELECT ''8'' AS n UNION ALL SELECT ''9'' UNION ALL SELECT ''A'' UNION ALL SELECT ''B'') n3
CROSS JOIN (SELECT ''0'' AS n UNION ALL SELECT ''1'' UNION ALL SELECT ''2'' UNION ALL SELECT ''3'' UNION ALL SELECT ''4'' UNION ALL SELECT ''5'' UNION ALL SELECT ''6'' UNION ALL SELECT ''7'' UNION ALL SELECT ''8'' UNION ALL SELECT ''9'' UNION ALL SELECT ''A'' UNION ALL SELECT ''B'' UNION ALL SELECT ''C'' UNION ALL SELECT ''D'' UNION ALL SELECT ''E'' UNION ALL SELECT ''F'') n4
CROSS JOIN (SELECT ''8'' AS n UNION ALL SELECT ''9'' UNION ALL SELECT ''A'' UNION ALL SELECT ''B'') n5
CROSS JOIN (SELECT ''0'' AS n UNION ALL SELECT ''1'' UNION ALL SELECT ''2'' UNION ALL SELECT ''3'' UNION ALL SELECT ''4'' UNION ALL SELECT ''5'' UNION ALL SELECT ''6'' UNION ALL SELECT ''7'' UNION ALL SELECT ''8'' UNION ALL SELECT ''9'' UNION ALL SELECT ''A'' UNION ALL SELECT ''B'' UNION ALL SELECT ''C'' UNION ALL SELECT ''D'' UNION ALL SELECT ''E'' UNION ALL SELECT ''F'') n6
UNION ALL
SELECT CONCAT(n1.n, n2.n, n3.n, n4.n) AS hex_val
FROM (SELECT ''C'' AS n UNION ALL SELECT ''D'') n1
CROSS JOIN (SELECT ''0'' AS n UNION ALL SELECT ''1'' UNION ALL SELECT ''2'' UNION ALL SELECT ''3'' UNION ALL SELECT ''4'' UNION ALL SELECT ''5'' UNION ALL SELECT ''6'' UNION ALL SELECT ''7'' UNION ALL SELECT ''8'' UNION ALL SELECT ''9'' UNION ALL SELECT ''A'' UNION ALL SELECT ''B'' UNION ALL SELECT ''C'' UNION ALL SELECT ''D'' UNION ALL SELECT ''E'' UNION ALL SELECT ''F'') n2
CROSS JOIN (SELECT ''8'' AS n UNION ALL SELECT ''9'' UNION ALL SELECT ''A'' UNION ALL SELECT ''B'') n3
CROSS JOIN (SELECT ''0'' AS n UNION ALL SELECT ''1'' UNION ALL SELECT ''2'' UNION ALL SELECT ''3'' UNION ALL SELECT ''4'' UNION ALL SELECT ''5'' UNION ALL SELECT ''6'' UNION ALL SELECT ''7'' UNION ALL SELECT ''8'' UNION ALL SELECT ''9'' UNION ALL SELECT ''A'' UNION ALL SELECT ''B'' UNION ALL SELECT ''C'' UNION ALL SELECT ''D'' UNION ALL SELECT ''E'' UNION ALL SELECT ''F'') n4
UNION ALL
SELECT CONCAT(n1.n, n2.n) AS hex_val
FROM (SELECT ''0'' AS n UNION ALL SELECT ''1'' UNION ALL SELECT ''2'' UNION ALL SELECT ''3'' UNION ALL SELECT ''4'' UNION ALL SELECT ''5'' UNION ALL SELECT ''6'' UNION ALL SELECT ''7'') n1
CROSS JOIN (SELECT ''0'' AS n UNION ALL SELECT ''1'' UNION ALL SELECT ''2'' UNION ALL SELECT ''3'' UNION ALL SELECT ''4'' UNION ALL SELECT ''5'' UNION ALL SELECT ''6'' UNION ALL SELECT ''7'' UNION ALL SELECT ''8'' UNION ALL SELECT ''9'' UNION ALL SELECT ''A'' UNION ALL SELECT ''B'' UNION ALL SELECT ''C'' UNION ALL SELECT ''D'' UNION ALL SELECT ''E'' UNION ALL SELECT ''F'') n2
';

SET @charset_chars = CONCAT(
  '(SELECT charset_char FROM ( ',
    'SELECT hex_val, CONVERT(UNHEX(hex_val) USING utf8mb4) AS utf8_char, ',
    'CONVERT(CONVERT(UNHEX(hex_val) USING utf8mb4) USING ', @db_charset, ') AS charset_char ',
    'FROM ( ', @all_chars, ' ) AS all_chars ',
    'HAVING utf8_char IS NOT NULL AND hex_val NOT BETWEEN ''EDA080'' AND ''EDBFBF'' ',
  ') AS valid_utf8_chars ',
  'WHERE charset_char IS NOT NULL AND (HEX(CONVERT(charset_char USING utf8mb4)) != ''3F'' OR hex_val = ''3F'') ',
')');


SET @SPACE=CONCAT('CONVERT('' '' USING ', @db_charset,')');
SET @ALPHABET=CONCAT('CONVERT(''a'' USING ', @db_charset,')');
SET @ALPHABET_PAIR=CONCAT('CONCAT(', @ALPHABET, ',', @ALPHABET, ')');
SET @SPACE_BETWEEN_ALPHABET=CONCAT('CONCAT(',@ALPHABET,',',@SPACE,',',@ALPHABET, ')');
SET @CHAR_BETWEEN_ALPHABET=CONCAT('CONCAT(', @ALPHABET , ',', 'charset_char,', @ALPHABET, ')');
SET @CHECK_SPACE=CONCAT(@CHAR_BETWEEN_ALPHABET, ' = ', @SPACE_BETWEEN_ALPHABET, ' COLLATE ', @db_collation);
SET @CHECK_EMPTY=CONCAT(@CHAR_BETWEEN_ALPHABET, ' = ', @ALPHABET_PAIR, ' COLLATE ', @db_collation);


-- Get the characters with their codepoints.
SET @charset_chars_with_codepoints = CONCAT(
    'SELECT ',
    '  CONV(HEX(CONVERT(charset_char using ', @db_charset ,' )), 16, 10) AS codepoint, charset_char, ',
    @CHAR_BETWEEN_ALPHABET, ' AS charset_char_non_trailing,',
     @CHECK_EMPTY,' AS is_empty,',
     @CHECK_SPACE, ' AS is_space ',
    '  FROM (', @charset_chars, ') AS charset_with_codepoints_query '
);



-- We use codepoint to map character of the string to a java code-point.
-- equivalent_codepoint is the minimum code point which is equal to a given character for the given @db_collation.
-- For example in any case insensitive collation, the equivalent_codepoint for 'a' will be the codepoint for 'A'
-- Mysql ignores empty characters coming in between a string for comparison. For example _utf8mb4`ab` will compare equal to CONCAT(_utf8mb4'a', CONVERT(UNHEX('001A') using utf8mb4), _utf8mb4'b')
-- Trailing spaces are ignored depending on whether a collation is PAD SPACE or not.
-- Note that there are various codepoints that can potentially represent a space, like the ascii space or non-breaking space (UNHEX(C2H0)) when the collation is Pad Space. These have same behavior to ascii space as far as trailing or non-trailing comparison is concerned.
SET @find_equivalents_query = CONCAT(
    ' SELECT *, ',
    ' FIRST_VALUE(CAST(codepoint AS SIGNED)) OVER (PARTITION BY charset_char COLLATE ', @db_collation, ', is_empty ORDER BY charset_char COLLATE ', @db_collation, ',CAST(codepoint AS SIGNED) ) AS equivalent_codepoint, ',
    ' FIRST_VALUE(charset_char) OVER (PARTITION BY charset_char COLLATE ', @db_collation, ', is_empty ORDER BY charset_char COLLATE ', @db_collation, ',CAST(codepoint AS SIGNED) ) AS equivalent_charset_char, ',
    ' FIRST_VALUE(CAST(codepoint AS SIGNED)) OVER (PARTITION BY charset_char_non_trailing COLLATE ', @db_collation, ', is_empty ORDER BY charset_char COLLATE ', @db_collation, ',CAST(codepoint AS SIGNED) ) AS equivalent_codepoint_non_trailing, ',
    ' FIRST_VALUE(charset_char) OVER (PARTITION BY charset_char_non_trailing COLLATE ', @db_collation, ', is_empty ORDER BY charset_char COLLATE ', @db_collation, ',CAST(codepoint AS SIGNED) ) AS equivalent_charset_char_non_trailing, ',
    ' FIRST_VALUE(CAST(codepoint AS SIGNED)) OVER (PARTITION BY charset_char COLLATE ', @db_collation, ', is_empty, is_space ORDER BY charset_char COLLATE ', @db_collation, ',CAST(codepoint AS SIGNED) ) AS equivalent_codepoint_pad_space, ',
    ' FIRST_VALUE(charset_char) OVER (PARTITION BY charset_char COLLATE ', @db_collation, ', is_empty, is_space ORDER BY charset_char COLLATE ', @db_collation, ',CAST(codepoint AS SIGNED) ) AS equivalent_charset_char_pad_space ',
    ' FROM ( ', @charset_chars_with_codepoints, ' ) AS find_equivalents',
    ' ORDER BY codepoint'
);

-- Find the rank of a code_point as per the collation ordering.
-- The `_pad_space` rank is the rank of the code_point at trailing position if pad-space comparison is enforced.
SET @find_rank_query = CONCAT(
    'SELECT *, ',
    ' DENSE_RANK() OVER (PARTITION BY is_empty ORDER BY equivalent_charset_char_non_trailing COLLATE ', @db_collation, ') - 1 AS codepoint_rank, ',
    ' DENSE_RANK() OVER (PARTITION BY is_empty, is_space ORDER BY equivalent_charset_char_pad_space COLLATE ', @db_collation, ') - 1 AS codepoint_rank_pad_space ',
    ' FROM ( ',
    @find_equivalents_query,
    ' ) AS find_rank ',
    ' ORDER BY CAST(codepoint AS SIGNED) ASC '
);

SET @output_query = CONCAT(
  ' SELECT * ',
  ' FROM (',
    @find_rank_query,
  ' ) AS output_query ',
  ' ORDER BY CAST(codepoint AS SIGNED) '
);

PREPARE stmt FROM @output_query;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
