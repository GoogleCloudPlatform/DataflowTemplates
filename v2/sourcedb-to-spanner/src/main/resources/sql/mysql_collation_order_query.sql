-- In this query, at a high level we sort a sequence of variable length code points as per the collation order of the database to understand how the code points for characters compare for the given collation.
-- We start with all possible variable length code points which represent a single characters, sort them by the collation order and condense the output to groups.
-- More details are explained in the query that follows.
-- Note that although the query appears big it completes in a time < 60 seconds as the db just has to do in-memory sorting of literals.
-- TODO(vardhanvthigle): Currently this query might not be compatible with MySQl 5.7. Change window functions with inner joins.

-- Unfortunately we can't use prepared statement to set variable values via jdbc.
-- The dataflow code will replace the below tags with the actual db charset and collation.
SET @db_charset = 'charset_replacement_tag';
SET @db_collation = 'collation_replacement_tag';



-- A union of single byte literals from 0x00 to 0xff.
SET @byte_literals = CONCAT(
      'SELECT ''00'' AS h UNION ALL SELECT ''01'' UNION ALL SELECT ''02'' UNION ALL SELECT ''03'' UNION ALL SELECT ''04'' UNION ALL SELECT ''05'' UNION ALL SELECT ''06'' UNION ALL SELECT ''07'' UNION ALL SELECT ''08'' UNION ALL SELECT ''09'' UNION ALL SELECT ''0a'' UNION ALL SELECT ''0b'' UNION ALL SELECT ''0c'' UNION ALL SELECT ''0d'' UNION ALL SELECT ''0e'' UNION ALL SELECT ''0f''',
'UNION ALL SELECT ''10'' AS h UNION ALL SELECT ''11'' UNION ALL SELECT ''12'' UNION ALL SELECT ''13'' UNION ALL SELECT ''14'' UNION ALL SELECT ''15'' UNION ALL SELECT ''16'' UNION ALL SELECT ''17'' UNION ALL SELECT ''18'' UNION ALL SELECT ''19'' UNION ALL SELECT ''1a'' UNION ALL SELECT ''1b'' UNION ALL SELECT ''1c'' UNION ALL SELECT ''1d'' UNION ALL SELECT ''1e'' UNION ALL SELECT ''1f''',
'UNION ALL SELECT ''20'' AS h UNION ALL SELECT ''21'' UNION ALL SELECT ''22'' UNION ALL SELECT ''23'' UNION ALL SELECT ''24'' UNION ALL SELECT ''25'' UNION ALL SELECT ''26'' UNION ALL SELECT ''27'' UNION ALL SELECT ''28'' UNION ALL SELECT ''29'' UNION ALL SELECT ''2a'' UNION ALL SELECT ''2b'' UNION ALL SELECT ''2c'' UNION ALL SELECT ''2d'' UNION ALL SELECT ''2e'' UNION ALL SELECT ''2f''',
'UNION ALL SELECT ''30'' AS h UNION ALL SELECT ''31'' UNION ALL SELECT ''32'' UNION ALL SELECT ''33'' UNION ALL SELECT ''34'' UNION ALL SELECT ''35'' UNION ALL SELECT ''36'' UNION ALL SELECT ''37'' UNION ALL SELECT ''38'' UNION ALL SELECT ''39'' UNION ALL SELECT ''3a'' UNION ALL SELECT ''3b'' UNION ALL SELECT ''3c'' UNION ALL SELECT ''3d'' UNION ALL SELECT ''3e'' UNION ALL SELECT ''3f''',
'UNION ALL SELECT ''40'' AS h UNION ALL SELECT ''41'' UNION ALL SELECT ''42'' UNION ALL SELECT ''43'' UNION ALL SELECT ''44'' UNION ALL SELECT ''45'' UNION ALL SELECT ''46'' UNION ALL SELECT ''47'' UNION ALL SELECT ''48'' UNION ALL SELECT ''49'' UNION ALL SELECT ''4a'' UNION ALL SELECT ''4b'' UNION ALL SELECT ''4c'' UNION ALL SELECT ''4d'' UNION ALL SELECT ''4e'' UNION ALL SELECT ''4f''',
'UNION ALL SELECT ''50'' AS h UNION ALL SELECT ''51'' UNION ALL SELECT ''52'' UNION ALL SELECT ''53'' UNION ALL SELECT ''54'' UNION ALL SELECT ''55'' UNION ALL SELECT ''56'' UNION ALL SELECT ''57'' UNION ALL SELECT ''58'' UNION ALL SELECT ''59'' UNION ALL SELECT ''5a'' UNION ALL SELECT ''5b'' UNION ALL SELECT ''5c'' UNION ALL SELECT ''5d'' UNION ALL SELECT ''5e'' UNION ALL SELECT ''5f''',
'UNION ALL SELECT ''60'' AS h UNION ALL SELECT ''61'' UNION ALL SELECT ''62'' UNION ALL SELECT ''63'' UNION ALL SELECT ''64'' UNION ALL SELECT ''65'' UNION ALL SELECT ''66'' UNION ALL SELECT ''67'' UNION ALL SELECT ''68'' UNION ALL SELECT ''69'' UNION ALL SELECT ''6a'' UNION ALL SELECT ''6b'' UNION ALL SELECT ''6c'' UNION ALL SELECT ''6d'' UNION ALL SELECT ''6e'' UNION ALL SELECT ''6f''',
'UNION ALL SELECT ''70'' AS h UNION ALL SELECT ''71'' UNION ALL SELECT ''72'' UNION ALL SELECT ''73'' UNION ALL SELECT ''74'' UNION ALL SELECT ''75'' UNION ALL SELECT ''76'' UNION ALL SELECT ''77'' UNION ALL SELECT ''78'' UNION ALL SELECT ''79'' UNION ALL SELECT ''7a'' UNION ALL SELECT ''7b'' UNION ALL SELECT ''7c'' UNION ALL SELECT ''7d'' UNION ALL SELECT ''7e'' UNION ALL SELECT ''7f''',
'UNION ALL SELECT ''80'' AS h UNION ALL SELECT ''81'' UNION ALL SELECT ''82'' UNION ALL SELECT ''83'' UNION ALL SELECT ''84'' UNION ALL SELECT ''85'' UNION ALL SELECT ''86'' UNION ALL SELECT ''87'' UNION ALL SELECT ''88'' UNION ALL SELECT ''89'' UNION ALL SELECT ''8a'' UNION ALL SELECT ''8b'' UNION ALL SELECT ''8c'' UNION ALL SELECT ''8d'' UNION ALL SELECT ''8e'' UNION ALL SELECT ''8f''',
'UNION ALL SELECT ''90'' AS h UNION ALL SELECT ''91'' UNION ALL SELECT ''92'' UNION ALL SELECT ''93'' UNION ALL SELECT ''94'' UNION ALL SELECT ''95'' UNION ALL SELECT ''96'' UNION ALL SELECT ''97'' UNION ALL SELECT ''98'' UNION ALL SELECT ''99'' UNION ALL SELECT ''9a'' UNION ALL SELECT ''9b'' UNION ALL SELECT ''9c'' UNION ALL SELECT ''9d'' UNION ALL SELECT ''9e'' UNION ALL SELECT ''9f''',
'UNION ALL SELECT ''a0'' AS h UNION ALL SELECT ''a1'' UNION ALL SELECT ''a2'' UNION ALL SELECT ''a3'' UNION ALL SELECT ''a4'' UNION ALL SELECT ''a5'' UNION ALL SELECT ''a6'' UNION ALL SELECT ''a7'' UNION ALL SELECT ''a8'' UNION ALL SELECT ''a9'' UNION ALL SELECT ''aa'' UNION ALL SELECT ''ab'' UNION ALL SELECT ''ac'' UNION ALL SELECT ''ad'' UNION ALL SELECT ''ae'' UNION ALL SELECT ''af''',
'UNION ALL SELECT ''b0'' AS h UNION ALL SELECT ''b1'' UNION ALL SELECT ''b2'' UNION ALL SELECT ''b3'' UNION ALL SELECT ''b4'' UNION ALL SELECT ''b5'' UNION ALL SELECT ''b6'' UNION ALL SELECT ''b7'' UNION ALL SELECT ''b8'' UNION ALL SELECT ''b9'' UNION ALL SELECT ''ba'' UNION ALL SELECT ''bb'' UNION ALL SELECT ''bc'' UNION ALL SELECT ''bd'' UNION ALL SELECT ''be'' UNION ALL SELECT ''bf''',
'UNION ALL SELECT ''c0'' AS h UNION ALL SELECT ''c1'' UNION ALL SELECT ''c2'' UNION ALL SELECT ''c3'' UNION ALL SELECT ''c4'' UNION ALL SELECT ''c5'' UNION ALL SELECT ''c6'' UNION ALL SELECT ''c7'' UNION ALL SELECT ''c8'' UNION ALL SELECT ''c9'' UNION ALL SELECT ''ca'' UNION ALL SELECT ''cb'' UNION ALL SELECT ''cc'' UNION ALL SELECT ''cd'' UNION ALL SELECT ''ce'' UNION ALL SELECT ''cf''',
'UNION ALL SELECT ''d0'' AS h UNION ALL SELECT ''d1'' UNION ALL SELECT ''d2'' UNION ALL SELECT ''d3'' UNION ALL SELECT ''d4'' UNION ALL SELECT ''d5'' UNION ALL SELECT ''d6'' UNION ALL SELECT ''d7'' UNION ALL SELECT ''d8'' UNION ALL SELECT ''d9'' UNION ALL SELECT ''da'' UNION ALL SELECT ''db'' UNION ALL SELECT ''dc'' UNION ALL SELECT ''dd'' UNION ALL SELECT ''de'' UNION ALL SELECT ''df''',
'UNION ALL SELECT ''e0'' AS h UNION ALL SELECT ''e1'' UNION ALL SELECT ''e2'' UNION ALL SELECT ''e3'' UNION ALL SELECT ''e4'' UNION ALL SELECT ''e5'' UNION ALL SELECT ''e6'' UNION ALL SELECT ''e7'' UNION ALL SELECT ''e8'' UNION ALL SELECT ''e9'' UNION ALL SELECT ''ea'' UNION ALL SELECT ''eb'' UNION ALL SELECT ''ec'' UNION ALL SELECT ''ed'' UNION ALL SELECT ''ee'' UNION ALL SELECT ''ef''',
'UNION ALL SELECT ''f0'' AS h UNION ALL SELECT ''f1'' UNION ALL SELECT ''f2'' UNION ALL SELECT ''f3'' UNION ALL SELECT ''f4'' UNION ALL SELECT ''f5'' UNION ALL SELECT ''f6'' UNION ALL SELECT ''f7'' UNION ALL SELECT ''f8'' UNION ALL SELECT ''f9'' UNION ALL SELECT ''fa'' UNION ALL SELECT ''fb'' UNION ALL SELECT ''fc'' UNION ALL SELECT ''fd'' UNION ALL SELECT ''fe'' UNION ALL SELECT ''ff'''
);

-- Four byte code points.
SET @four_byte_codepoints = CONCAT(
  '(SELECT * FROM (SELECT ',
    'CONVERT(UNHEX(CONCAT(t1.h, t2.h, t3.h, t4.h)) USING ', @db_charset, ') AS charset_char ',
    'FROM (', @byte_literals, ') AS t1 ',
    'LEFT JOIN (', @byte_literals, ') AS t2 ON 1=1 ',
    'LEFT JOIN (', @byte_literals, ') AS t3 ON 1=1 ',
    'LEFT JOIN (', @byte_literals, ') AS t4 ON 1=1 ',
    ') AS dt ',
    'WHERE CHAR_LENGTH(charset_char) <= 1 AND charset_char IS NOT NULL'
  ')'
);

-- Three byte code points.
SET @three_byte_codepoints = CONCAT(
  '(SELECT * FROM (SELECT ',
    'CONVERT(UNHEX(CONCAT(t1.h, t2.h, t3.h)) USING ', @db_charset, ') AS charset_char ',
    'FROM (', @byte_literals, ') AS t1 ',
    'LEFT JOIN (', @byte_literals, ') AS t2 ON 1=1 ',
    'LEFT JOIN (', @byte_literals, ') AS t3 ON 1=1 ',
    ') AS dt ',
    'WHERE CHAR_LENGTH(charset_char) <= 1 AND charset_char IS NOT NULL'
  ')'
);

-- Two byte code points.
SET @two_byte_codepoints = CONCAT(
  '(SELECT * FROM (SELECT ',
    'CONVERT(UNHEX(CONCAT(t1.h, t2.h)) USING ', @db_charset, ') AS charset_char ',
    'FROM (', @byte_literals, ') AS t1 ',
    'LEFT JOIN (', @byte_literals, ') AS t2 ON 1=1 ',
    ') AS dt ',
    'WHERE CHAR_LENGTH(charset_char) <= 1 AND charset_char IS NOT NULL'
  ')'
);

-- Single byte code points.
SET @one_byte_codepoints = CONCAT(
  '(SELECT * FROM (SELECT ',
    'CONVERT(UNHEX(t1.h) USING ', @db_charset, ') AS charset_char ',
    'FROM (', @byte_literals, ') AS t1',
    ') AS dt ', -- derived table
    'WHERE CHAR_LENGTH(charset_char) <= 1 AND charset_char IS NOT NULL'
  ')'
);

-- all variable length code points representing a single character within the @db_charset from length 0 till 4.
SET @charset_chars = CONCAT(@three_byte_codepoints, ' UNION ALL ', @two_byte_codepoints, ' UNION ALL ', @one_byte_codepoints);

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
