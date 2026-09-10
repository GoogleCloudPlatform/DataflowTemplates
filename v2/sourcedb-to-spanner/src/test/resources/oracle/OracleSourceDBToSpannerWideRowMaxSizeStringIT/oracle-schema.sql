CREATE TABLE "WideRowTable" (
  "id" INTEGER PRIMARY KEY,
  "max_string_col_to_bytes" BLOB,
  "max_string_col_to_str" CLOB
)
-- SPLIT --
DECLARE
  v_blob BLOB;
  v_clob CLOB;
BEGIN
  DBMS_LOB.CREATETEMPORARY(v_blob, TRUE);
  DBMS_LOB.CREATETEMPORARY(v_clob, TRUE);
  FOR i IN 1..312 LOOP
    DBMS_LOB.WRITEAPPEND(v_blob, 32000, UTL_RAW.CAST_TO_RAW(RPAD('a', 32000, 'a')));
  END LOOP;
  DBMS_LOB.WRITEAPPEND(v_blob, 16000, UTL_RAW.CAST_TO_RAW(RPAD('a', 16000, 'a')));
  FOR i IN 1..78 LOOP
    DBMS_LOB.WRITEAPPEND(v_clob, 32000, RPAD('a', 32000, 'a'));
  END LOOP;
  DBMS_LOB.WRITEAPPEND(v_clob, 4000, RPAD('a', 4000, 'a'));
  INSERT INTO "WideRowTable" ("id", "max_string_col_to_bytes", "max_string_col_to_str") VALUES (1, v_blob, v_clob);
  DBMS_LOB.FREETEMPORARY(v_blob);
  DBMS_LOB.FREETEMPORARY(v_clob);
END;
-- SPLIT --
DECLARE
  v_blob BLOB;
BEGIN
  DBMS_LOB.CREATETEMPORARY(v_blob, TRUE);
  FOR i IN 1..655 LOOP
    DBMS_LOB.WRITEAPPEND(v_blob, 32000, UTL_RAW.CAST_TO_RAW(RPAD('b', 32000, 'b')));
  END LOOP;
  DBMS_LOB.WRITEAPPEND(v_blob, 11520, UTL_RAW.CAST_TO_RAW(RPAD('b', 11520, 'b')));
  INSERT INTO "WideRowTable" ("id", "max_string_col_to_bytes", "max_string_col_to_str") VALUES (2, v_blob, NULL);
  DBMS_LOB.FREETEMPORARY(v_blob);
END;
-- SPLIT --
DECLARE
  v_clob CLOB;
BEGIN
  DBMS_LOB.CREATETEMPORARY(v_clob, TRUE);
  FOR i IN 1..90 LOOP
    DBMS_LOB.WRITEAPPEND(v_clob, 32000, RPAD('b', 32000, 'b'));
  END LOOP;
  DBMS_LOB.WRITEAPPEND(v_clob, 3584, RPAD('b', 3584, 'b'));
  INSERT INTO "WideRowTable" ("id", "max_string_col_to_bytes", "max_string_col_to_str") VALUES (3, NULL, v_clob);
  DBMS_LOB.FREETEMPORARY(v_clob);
END;
