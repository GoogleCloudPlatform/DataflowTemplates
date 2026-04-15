-- Collation order query for MySQL 5.7+ and 8.0+.
--
-- At a high level: sort all valid single-character codepoints for the charset by the database's
-- own collation order, then compute for each character:
--   - its collation-equivalent representative (the char with the smallest codepoint that
--     compares equal to it under the collation), and
--   - its 0-based dense rank in that collation ordering.
--
-- The ranks are used by Java (CollationMapper) to map strings to BigIntegers so that
-- midpoints can be computed for binary-search style range splitting.
--
-- Implementation note - compatibility with MySQL 5.7 and 8.0:
--   Earlier versions of this query used FIRST_VALUE() and DENSE_RANK() window functions which
--   require MySQL 8.0+. This version replaces them with temporary tables, GROUP BY joins, and
--   correlated COUNT(DISTINCT) subqueries, which work on MySQL 5.7+ and 8.0+.
--
-- Implementation note - performance:
--   The expensive part is building all valid single-character codepoints, which involves cross-
--   joining 256-value byte-literal sets. By materialising this into _tmp_ccp first, the cross-join
--   is only executed once regardless of how many self-joins follow. The original nested-subquery
--   form re-evaluated the same cross-join for every window partition. Total runtime is typically
--   under 60 seconds since all operations are in-memory on literal values with no table I/O.
--
-- Unfortunately we can't use prepared statements to set variable values via JDBC.
-- The dataflow code replaces the tags below with the actual db charset and collation.
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

-- Four byte code points (defined but not included in @charset_chars: 256^4 combinations would
-- be too expensive even after filtering; typical charset chars need at most 3 bytes).
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
    ') AS dt ',
    'WHERE CHAR_LENGTH(charset_char) <= 1 AND charset_char IS NOT NULL'
  ')'
);

-- All variable length code points representing a single character within @db_charset (up to 3 bytes).
SET @charset_chars = CONCAT(@three_byte_codepoints, ' UNION ALL ', @two_byte_codepoints, ' UNION ALL ', @one_byte_codepoints);

SET @SPACE=CONCAT('CONVERT('' '' USING ', @db_charset,')');
SET @ALPHABET=CONCAT('CONVERT(''a'' USING ', @db_charset,')');
SET @ALPHABET_PAIR=CONCAT('CONCAT(', @ALPHABET, ',', @ALPHABET, ')');
SET @SPACE_BETWEEN_ALPHABET=CONCAT('CONCAT(',@ALPHABET,',',@SPACE,',',@ALPHABET, ')');
SET @CHAR_BETWEEN_ALPHABET=CONCAT('CONCAT(', @ALPHABET , ',', 'charset_char,', @ALPHABET, ')');
SET @CHECK_SPACE=CONCAT(@CHAR_BETWEEN_ALPHABET, ' = ', @SPACE_BETWEEN_ALPHABET, ' COLLATE ', @db_collation);
SET @CHECK_EMPTY=CONCAT(@CHAR_BETWEEN_ALPHABET, ' = ', @ALPHABET_PAIR, ' COLLATE ', @db_collation);

-- Builds the raw charset-chars-with-codepoints query string.
-- codepoint_num is pre-cast to SIGNED so all subsequent joins and ORDER BY use the integer
-- directly, avoiding repeated CAST(... AS SIGNED) expressions.
SET @charset_chars_with_codepoints = CONCAT(
    'SELECT ',
    '  CAST(CONV(HEX(CONVERT(charset_char USING ', @db_charset, ')), 16, 10) AS SIGNED) AS codepoint_num, ',
    '  charset_char, ',
    @CHAR_BETWEEN_ALPHABET, ' AS charset_char_non_trailing, ',
    @CHECK_EMPTY, ' AS is_empty, ',
    @CHECK_SPACE, ' AS is_space ',
    'FROM (', @charset_chars, ') AS charset_raw'
);



-- ============================================================
-- Step 1: Materialise charset chars into a temporary table.
-- ============================================================
-- Doing this once avoids re-running the expensive three-way cross-join for every subsequent
-- self-join in the equivalents and ranking queries.
-- The DROP ensures clean state on retries or connection reuse.
DROP TEMPORARY TABLE IF EXISTS _tmp_ccp;
SET @q_create_ccp = CONCAT(
    'CREATE TEMPORARY TABLE _tmp_ccp AS SELECT * FROM (',
    @charset_chars_with_codepoints,
    ') AS _ccp_inner'
);
PREPARE stmt_ccp FROM @q_create_ccp;
EXECUTE stmt_ccp;
DEALLOCATE PREPARE stmt_ccp;
-- Primary key on codepoint_num lets the equivalents query join back efficiently
-- when looking up the representative character for each group minimum.
ALTER TABLE _tmp_ccp ADD PRIMARY KEY (codepoint_num);



-- ============================================================
-- Step 2: Find collation-equivalents via GROUP BY + JOIN.
-- ============================================================
-- For each character c, its "equivalent" is the character with the smallest codepoint that
-- compares equal to c under the collation. Three equivalence variants are needed:
--
--   equivalent_charset_char           - trailing position  (partition: charset_char COLLATE col, is_empty)
--   equivalent_charset_char_non_trailing - non-trailing    (partition: charset_char_non_trailing COLLATE col, is_empty)
--   equivalent_charset_char_pad_space - pad-space trailing (partition: charset_char COLLATE col, is_empty, is_space)
--
-- Replaces: FIRST_VALUE(...) OVER (PARTITION BY ... ORDER BY codepoint ASC)
-- Equivalent because within each partition all rows share the same collation value, so
-- FIRST_VALUE ordered by codepoint = MIN(codepoint) in that group.
DROP TEMPORARY TABLE IF EXISTS _tmp_equiv;
SET @q_create_equiv = CONCAT(
  'CREATE TEMPORARY TABLE _tmp_equiv AS',
  ' SELECT',
  '   t1.codepoint_num,',
  '   t1.charset_char,',
  '   t1.is_empty,',
  '   t1.is_space,',
  '   e1.charset_char AS equivalent_charset_char,',
  '   e2.charset_char AS equivalent_charset_char_non_trailing,',
  '   e3.charset_char AS equivalent_charset_char_pad_space',
  ' FROM _tmp_ccp t1',

  -- G1: trailing-position equivalence
  ' JOIN (',
  '   SELECT charset_char COLLATE ', @db_collation, ' AS grp,',
  '          is_empty,',
  '          MIN(codepoint_num) AS min_cpn',
  '   FROM _tmp_ccp',
  '   GROUP BY charset_char COLLATE ', @db_collation, ', is_empty',
  ' ) g1',
  '   ON t1.charset_char COLLATE ', @db_collation, ' = g1.grp',
  '   AND t1.is_empty = g1.is_empty',
  ' JOIN _tmp_ccp e1 ON e1.codepoint_num = g1.min_cpn',

  -- G2: non-trailing-position equivalence
  ' JOIN (',
  '   SELECT charset_char_non_trailing COLLATE ', @db_collation, ' AS grp,',
  '          is_empty,',
  '          MIN(codepoint_num) AS min_cpn',
  '   FROM _tmp_ccp',
  '   GROUP BY charset_char_non_trailing COLLATE ', @db_collation, ', is_empty',
  ' ) g2',
  '   ON t1.charset_char_non_trailing COLLATE ', @db_collation, ' = g2.grp',
  '   AND t1.is_empty = g2.is_empty',
  ' JOIN _tmp_ccp e2 ON e2.codepoint_num = g2.min_cpn',

  -- G3: pad-space trailing-position equivalence
  ' JOIN (',
  '   SELECT charset_char COLLATE ', @db_collation, ' AS grp,',
  '          is_empty,',
  '          is_space,',
  '          MIN(codepoint_num) AS min_cpn',
  '   FROM _tmp_ccp',
  '   GROUP BY charset_char COLLATE ', @db_collation, ', is_empty, is_space',
  ' ) g3',
  '   ON t1.charset_char COLLATE ', @db_collation, ' = g3.grp',
  '   AND t1.is_empty = g3.is_empty',
  '   AND t1.is_space = g3.is_space',
  ' JOIN _tmp_ccp e3 ON e3.codepoint_num = g3.min_cpn'
);
PREPARE stmt_equiv FROM @q_create_equiv;
EXECUTE stmt_equiv;
DEALLOCATE PREPARE stmt_equiv;



-- ============================================================
-- Step 3: Compute 0-based dense ranks and emit final output.
-- ============================================================
-- For each character, codepoint_rank is the number of distinct collation-equivalent groups
-- that sort strictly before it. This equals DENSE_RANK() - 1.
--
-- Replaces: DENSE_RANK() OVER (PARTITION BY is_empty ORDER BY equivalent_charset_char_non_trailing COLLATE col) - 1
-- Equivalent because COUNT(DISTINCT values < x) = number of distinct predecessors = dense rank - 1.
--
-- The correlated subqueries operate on _tmp_equiv which is already a small in-memory table
-- (a few thousand rows for typical charsets), so the N^2 cross-product is fast in practice.
--
-- Output columns consumed by CollationOrderRow.fromRS():
--   charset_char, equivalent_charset_char, is_empty, is_space,
--   equivalent_charset_char_pad_space, codepoint_rank, codepoint_rank_pad_space
SET @q_output = CONCAT(
  'SELECT',
  '   t.charset_char,',
  '   t.equivalent_charset_char,',
  '   t.is_empty,',
  '   t.is_space,',
  '   t.equivalent_charset_char_pad_space,',

  -- codepoint_rank: 0-based dense rank by non-trailing collation order within is_empty group
  '   (SELECT COUNT(DISTINCT r.equivalent_charset_char_non_trailing COLLATE ', @db_collation, ')',
  '    FROM _tmp_equiv r',
  '    WHERE r.is_empty = t.is_empty',
  '    AND r.equivalent_charset_char_non_trailing COLLATE ', @db_collation,
  '        < t.equivalent_charset_char_non_trailing COLLATE ', @db_collation,
  '   ) AS codepoint_rank,',

  -- codepoint_rank_pad_space: 0-based dense rank by pad-space collation order within (is_empty, is_space) group
  '   (SELECT COUNT(DISTINCT r.equivalent_charset_char_pad_space COLLATE ', @db_collation, ')',
  '    FROM _tmp_equiv r',
  '    WHERE r.is_empty = t.is_empty',
  '    AND r.is_space = t.is_space',
  '    AND r.equivalent_charset_char_pad_space COLLATE ', @db_collation,
  '        < t.equivalent_charset_char_pad_space COLLATE ', @db_collation,
  '   ) AS codepoint_rank_pad_space',

  ' FROM _tmp_equiv t',
  ' ORDER BY t.codepoint_num'
);
PREPARE stmt_output FROM @q_output;
EXECUTE stmt_output;
DEALLOCATE PREPARE stmt_output;



-- ============================================================
-- Cleanup: drop temporary tables.
-- ============================================================
-- These DROPs are best-effort; if the connection closes before they run, the temp tables are
-- automatically dropped with the connection. The DROP IF EXISTS at Step 1 and Step 2 above
-- handles leftover tables from any prior failed run on the same connection.
DROP TEMPORARY TABLE IF EXISTS _tmp_ccp;
DROP TEMPORARY TABLE IF EXISTS _tmp_equiv;
