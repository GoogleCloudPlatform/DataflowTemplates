
CREATE TABLE "varchar2_table" ("id" NUMBER PRIMARY KEY, "varchar2_col" VARCHAR2(4000));
INSERT INTO "varchar2_table" ("id", "varchar2_col") VALUES (1, '');
INSERT INTO "varchar2_table" ("id", "varchar2_col") VALUES (2, ' ');
INSERT INTO "varchar2_table" ("id", "varchar2_col") VALUES (3, 'DROP TABLE');
INSERT INTO "varchar2_table" ("id", "varchar2_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "varchar2_to_string_table" ("id" NUMBER PRIMARY KEY, "varchar2_col" VARCHAR2(4000));
INSERT INTO "varchar2_to_string_table" ("id", "varchar2_col") VALUES (1, '');
INSERT INTO "varchar2_to_string_table" ("id", "varchar2_col") VALUES (2, ' ');
INSERT INTO "varchar2_to_string_table" ("id", "varchar2_col") VALUES (3, 'DROP TABLE');
INSERT INTO "varchar2_to_string_table" ("id", "varchar2_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "varchar2_to_bytes_table" ("id" NUMBER PRIMARY KEY, "varchar2_col" VARCHAR2(4000));
INSERT INTO "varchar2_to_bytes_table" ("id", "varchar2_col") VALUES (1, '');
INSERT INTO "varchar2_to_bytes_table" ("id", "varchar2_col") VALUES (2, ' ');
INSERT INTO "varchar2_to_bytes_table" ("id", "varchar2_col") VALUES (3, 'DROP TABLE');
INSERT INTO "varchar2_to_bytes_table" ("id", "varchar2_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "varchar_table" ("id" NUMBER PRIMARY KEY, "varchar_col" VARCHAR(4000));
INSERT INTO "varchar_table" ("id", "varchar_col") VALUES (1, '');
INSERT INTO "varchar_table" ("id", "varchar_col") VALUES (2, ' ');
INSERT INTO "varchar_table" ("id", "varchar_col") VALUES (3, 'DROP TABLE');
INSERT INTO "varchar_table" ("id", "varchar_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "varchar_to_string_table" ("id" NUMBER PRIMARY KEY, "varchar_col" VARCHAR(4000));
INSERT INTO "varchar_to_string_table" ("id", "varchar_col") VALUES (1, '');
INSERT INTO "varchar_to_string_table" ("id", "varchar_col") VALUES (2, ' ');
INSERT INTO "varchar_to_string_table" ("id", "varchar_col") VALUES (3, 'DROP TABLE');
INSERT INTO "varchar_to_string_table" ("id", "varchar_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "varchar_to_bytes_table" ("id" NUMBER PRIMARY KEY, "varchar_col" VARCHAR(4000));
INSERT INTO "varchar_to_bytes_table" ("id", "varchar_col") VALUES (1, '');
INSERT INTO "varchar_to_bytes_table" ("id", "varchar_col") VALUES (2, ' ');
INSERT INTO "varchar_to_bytes_table" ("id", "varchar_col") VALUES (3, 'DROP TABLE');
INSERT INTO "varchar_to_bytes_table" ("id", "varchar_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "char_table" ("id" NUMBER PRIMARY KEY, "char_col" CHAR(2000));
INSERT INTO "char_table" ("id", "char_col") VALUES (1, '');
INSERT INTO "char_table" ("id", "char_col") VALUES (2, ' ');
INSERT INTO "char_table" ("id", "char_col") VALUES (3, 'DROP TABLE');
INSERT INTO "char_table" ("id", "char_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "char_to_string_table" ("id" NUMBER PRIMARY KEY, "char_col" CHAR(2000));
INSERT INTO "char_to_string_table" ("id", "char_col") VALUES (1, '');
INSERT INTO "char_to_string_table" ("id", "char_col") VALUES (2, ' ');
INSERT INTO "char_to_string_table" ("id", "char_col") VALUES (3, 'DROP TABLE');
INSERT INTO "char_to_string_table" ("id", "char_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "char_to_bytes_table" ("id" NUMBER PRIMARY KEY, "char_col" CHAR(2000));
INSERT INTO "char_to_bytes_table" ("id", "char_col") VALUES (1, '');
INSERT INTO "char_to_bytes_table" ("id", "char_col") VALUES (2, ' ');
INSERT INTO "char_to_bytes_table" ("id", "char_col") VALUES (3, 'DROP TABLE');
INSERT INTO "char_to_bytes_table" ("id", "char_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "character_table" ("id" NUMBER PRIMARY KEY, "character_col" CHARACTER(2000));
INSERT INTO "character_table" ("id", "character_col") VALUES (1, '');
INSERT INTO "character_table" ("id", "character_col") VALUES (2, ' ');
INSERT INTO "character_table" ("id", "character_col") VALUES (3, 'DROP TABLE');
INSERT INTO "character_table" ("id", "character_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "character_to_string_table" ("id" NUMBER PRIMARY KEY, "character_col" CHARACTER(2000));
INSERT INTO "character_to_string_table" ("id", "character_col") VALUES (1, '');
INSERT INTO "character_to_string_table" ("id", "character_col") VALUES (2, ' ');
INSERT INTO "character_to_string_table" ("id", "character_col") VALUES (3, 'DROP TABLE');
INSERT INTO "character_to_string_table" ("id", "character_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "character_to_bytes_table" ("id" NUMBER PRIMARY KEY, "character_col" CHARACTER(2000));
INSERT INTO "character_to_bytes_table" ("id", "character_col") VALUES (1, '');
INSERT INTO "character_to_bytes_table" ("id", "character_col") VALUES (2, ' ');
INSERT INTO "character_to_bytes_table" ("id", "character_col") VALUES (3, 'DROP TABLE');
INSERT INTO "character_to_bytes_table" ("id", "character_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "nvarchar2_table" ("id" NUMBER PRIMARY KEY, "nvarchar2_col" NVARCHAR2(2000));
INSERT INTO "nvarchar2_table" ("id", "nvarchar2_col") VALUES (1, '');
INSERT INTO "nvarchar2_table" ("id", "nvarchar2_col") VALUES (2, ' ');
INSERT INTO "nvarchar2_table" ("id", "nvarchar2_col") VALUES (3, 'DROP TABLE');
INSERT INTO "nvarchar2_table" ("id", "nvarchar2_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "nvarchar2_to_string_table" ("id" NUMBER PRIMARY KEY, "nvarchar2_col" NVARCHAR2(2000));
INSERT INTO "nvarchar2_to_string_table" ("id", "nvarchar2_col") VALUES (1, '');
INSERT INTO "nvarchar2_to_string_table" ("id", "nvarchar2_col") VALUES (2, ' ');
INSERT INTO "nvarchar2_to_string_table" ("id", "nvarchar2_col") VALUES (3, 'DROP TABLE');
INSERT INTO "nvarchar2_to_string_table" ("id", "nvarchar2_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "nvarchar2_to_bytes_table" ("id" NUMBER PRIMARY KEY, "nvarchar2_col" NVARCHAR2(2000));
INSERT INTO "nvarchar2_to_bytes_table" ("id", "nvarchar2_col") VALUES (1, '');
INSERT INTO "nvarchar2_to_bytes_table" ("id", "nvarchar2_col") VALUES (2, ' ');
INSERT INTO "nvarchar2_to_bytes_table" ("id", "nvarchar2_col") VALUES (3, 'DROP TABLE');
INSERT INTO "nvarchar2_to_bytes_table" ("id", "nvarchar2_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "nchar_table" ("id" NUMBER PRIMARY KEY, "nchar_col" NCHAR(1000));
INSERT INTO "nchar_table" ("id", "nchar_col") VALUES (1, '');
INSERT INTO "nchar_table" ("id", "nchar_col") VALUES (2, ' ');
INSERT INTO "nchar_table" ("id", "nchar_col") VALUES (3, 'DROP TABLE');
INSERT INTO "nchar_table" ("id", "nchar_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "nchar_to_string_table" ("id" NUMBER PRIMARY KEY, "nchar_col" NCHAR(1000));
INSERT INTO "nchar_to_string_table" ("id", "nchar_col") VALUES (1, '');
INSERT INTO "nchar_to_string_table" ("id", "nchar_col") VALUES (2, ' ');
INSERT INTO "nchar_to_string_table" ("id", "nchar_col") VALUES (3, 'DROP TABLE');
INSERT INTO "nchar_to_string_table" ("id", "nchar_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "nchar_to_bytes_table" ("id" NUMBER PRIMARY KEY, "nchar_col" NCHAR(1000));
INSERT INTO "nchar_to_bytes_table" ("id", "nchar_col") VALUES (1, '');
INSERT INTO "nchar_to_bytes_table" ("id", "nchar_col") VALUES (2, ' ');
INSERT INTO "nchar_to_bytes_table" ("id", "nchar_col") VALUES (3, 'DROP TABLE');
INSERT INTO "nchar_to_bytes_table" ("id", "nchar_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "nchar_varying_table" ("id" NUMBER PRIMARY KEY, "nchar_varying_col" NVARCHAR2(1000));
INSERT INTO "nchar_varying_table" ("id", "nchar_varying_col") VALUES (1, '');
INSERT INTO "nchar_varying_table" ("id", "nchar_varying_col") VALUES (2, ' ');
INSERT INTO "nchar_varying_table" ("id", "nchar_varying_col") VALUES (3, 'DROP TABLE');
INSERT INTO "nchar_varying_table" ("id", "nchar_varying_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "nchar_varying_to_string_table" ("id" NUMBER PRIMARY KEY, "nchar_varying_col" NVARCHAR2(1000));
INSERT INTO "nchar_varying_to_string_table" ("id", "nchar_varying_col") VALUES (1, '');
INSERT INTO "nchar_varying_to_string_table" ("id", "nchar_varying_col") VALUES (2, ' ');
INSERT INTO "nchar_varying_to_string_table" ("id", "nchar_varying_col") VALUES (3, 'DROP TABLE');
INSERT INTO "nchar_varying_to_string_table" ("id", "nchar_varying_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "nchar_varying_to_bytes_table" ("id" NUMBER PRIMARY KEY, "nchar_varying_col" NVARCHAR2(1000));
INSERT INTO "nchar_varying_to_bytes_table" ("id", "nchar_varying_col") VALUES (1, '');
INSERT INTO "nchar_varying_to_bytes_table" ("id", "nchar_varying_col") VALUES (2, ' ');
INSERT INTO "nchar_varying_to_bytes_table" ("id", "nchar_varying_col") VALUES (3, 'DROP TABLE');
INSERT INTO "nchar_varying_to_bytes_table" ("id", "nchar_varying_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "national_character_table" ("id" NUMBER PRIMARY KEY, "national_character_col" NATIONAL CHARACTER(1000));
INSERT INTO "national_character_table" ("id", "national_character_col") VALUES (1, '');
INSERT INTO "national_character_table" ("id", "national_character_col") VALUES (2, ' ');
INSERT INTO "national_character_table" ("id", "national_character_col") VALUES (3, 'DROP TABLE');
INSERT INTO "national_character_table" ("id", "national_character_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "national_character_to_string_table" ("id" NUMBER PRIMARY KEY, "national_character_col" NATIONAL CHARACTER(1000));
INSERT INTO "national_character_to_string_table" ("id", "national_character_col") VALUES (1, '');
INSERT INTO "national_character_to_string_table" ("id", "national_character_col") VALUES (2, ' ');
INSERT INTO "national_character_to_string_table" ("id", "national_character_col") VALUES (3, 'DROP TABLE');
INSERT INTO "national_character_to_string_table" ("id", "national_character_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "national_character_to_bytes_table" ("id" NUMBER PRIMARY KEY, "national_character_col" NATIONAL CHARACTER(1000));
INSERT INTO "national_character_to_bytes_table" ("id", "national_character_col") VALUES (1, '');
INSERT INTO "national_character_to_bytes_table" ("id", "national_character_col") VALUES (2, ' ');
INSERT INTO "national_character_to_bytes_table" ("id", "national_character_col") VALUES (3, 'DROP TABLE');
INSERT INTO "national_character_to_bytes_table" ("id", "national_character_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "national_char_table" ("id" NUMBER PRIMARY KEY, "national_char_col" NATIONAL CHAR(1000));
INSERT INTO "national_char_table" ("id", "national_char_col") VALUES (1, '');
INSERT INTO "national_char_table" ("id", "national_char_col") VALUES (2, ' ');
INSERT INTO "national_char_table" ("id", "national_char_col") VALUES (3, 'DROP TABLE');
INSERT INTO "national_char_table" ("id", "national_char_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "national_char_to_string_table" ("id" NUMBER PRIMARY KEY, "national_char_col" NATIONAL CHAR(1000));
INSERT INTO "national_char_to_string_table" ("id", "national_char_col") VALUES (1, '');
INSERT INTO "national_char_to_string_table" ("id", "national_char_col") VALUES (2, ' ');
INSERT INTO "national_char_to_string_table" ("id", "national_char_col") VALUES (3, 'DROP TABLE');
INSERT INTO "national_char_to_string_table" ("id", "national_char_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "national_char_to_bytes_table" ("id" NUMBER PRIMARY KEY, "national_char_col" NATIONAL CHAR(1000));
INSERT INTO "national_char_to_bytes_table" ("id", "national_char_col") VALUES (1, '');
INSERT INTO "national_char_to_bytes_table" ("id", "national_char_col") VALUES (2, ' ');
INSERT INTO "national_char_to_bytes_table" ("id", "national_char_col") VALUES (3, 'DROP TABLE');
INSERT INTO "national_char_to_bytes_table" ("id", "national_char_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "national_character_varying_table" ("id" NUMBER PRIMARY KEY, "national_character_varying_col" NVARCHAR2(1000));
INSERT INTO "national_character_varying_table" ("id", "national_character_varying_col") VALUES (1, '');
INSERT INTO "national_character_varying_table" ("id", "national_character_varying_col") VALUES (2, ' ');
INSERT INTO "national_character_varying_table" ("id", "national_character_varying_col") VALUES (3, 'DROP TABLE');
INSERT INTO "national_character_varying_table" ("id", "national_character_varying_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "national_character_varying_to_string_table" ("id" NUMBER PRIMARY KEY, "national_character_varying_col" NVARCHAR2(1000));
INSERT INTO "national_character_varying_to_string_table" ("id", "national_character_varying_col") VALUES (1, '');
INSERT INTO "national_character_varying_to_string_table" ("id", "national_character_varying_col") VALUES (2, ' ');
INSERT INTO "national_character_varying_to_string_table" ("id", "national_character_varying_col") VALUES (3, 'DROP TABLE');
INSERT INTO "national_character_varying_to_string_table" ("id", "national_character_varying_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "national_character_varying_to_bytes_table" ("id" NUMBER PRIMARY KEY, "national_character_varying_col" NVARCHAR2(1000));
INSERT INTO "national_character_varying_to_bytes_table" ("id", "national_character_varying_col") VALUES (1, '');
INSERT INTO "national_character_varying_to_bytes_table" ("id", "national_character_varying_col") VALUES (2, ' ');
INSERT INTO "national_character_varying_to_bytes_table" ("id", "national_character_varying_col") VALUES (3, 'DROP TABLE');
INSERT INTO "national_character_varying_to_bytes_table" ("id", "national_character_varying_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "national_char_varying_table" ("id" NUMBER PRIMARY KEY, "national_char_varying_col" NVARCHAR2(1000));
INSERT INTO "national_char_varying_table" ("id", "national_char_varying_col") VALUES (1, '');
INSERT INTO "national_char_varying_table" ("id", "national_char_varying_col") VALUES (2, ' ');
INSERT INTO "national_char_varying_table" ("id", "national_char_varying_col") VALUES (3, 'DROP TABLE');
INSERT INTO "national_char_varying_table" ("id", "national_char_varying_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "national_char_varying_to_string_table" ("id" NUMBER PRIMARY KEY, "national_char_varying_col" NVARCHAR2(1000));
INSERT INTO "national_char_varying_to_string_table" ("id", "national_char_varying_col") VALUES (1, '');
INSERT INTO "national_char_varying_to_string_table" ("id", "national_char_varying_col") VALUES (2, ' ');
INSERT INTO "national_char_varying_to_string_table" ("id", "national_char_varying_col") VALUES (3, 'DROP TABLE');
INSERT INTO "national_char_varying_to_string_table" ("id", "national_char_varying_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "national_char_varying_to_bytes_table" ("id" NUMBER PRIMARY KEY, "national_char_varying_col" NVARCHAR2(1000));
INSERT INTO "national_char_varying_to_bytes_table" ("id", "national_char_varying_col") VALUES (1, '');
INSERT INTO "national_char_varying_to_bytes_table" ("id", "national_char_varying_col") VALUES (2, ' ');
INSERT INTO "national_char_varying_to_bytes_table" ("id", "national_char_varying_col") VALUES (3, 'DROP TABLE');
INSERT INTO "national_char_varying_to_bytes_table" ("id", "national_char_varying_col") VALUES (4, RPAD('A', 1000, 'A'));
CREATE TABLE "number_table" ("id" NUMBER PRIMARY KEY, "number_col" NUMBER);
INSERT INTO "number_table" ("id", "number_col") VALUES (1, 922337203685477);
INSERT INTO "number_table" ("id", "number_col") VALUES (2, -922337203685477);
INSERT INTO "number_table" ("id", "number_col") VALUES (3, 0);
INSERT INTO "number_table" ("id", "number_col") VALUES (4, 922337203685476);
INSERT INTO "number_table" ("id", "number_col") VALUES (5, -922337203685476);
INSERT INTO "number_table" ("id", "number_col") VALUES (6, NULL);
CREATE TABLE "number_to_numeric_table" ("id" NUMBER PRIMARY KEY, "number_col" NUMBER);
INSERT INTO "number_to_numeric_table" ("id", "number_col") VALUES (1, 922337203685477);
INSERT INTO "number_to_numeric_table" ("id", "number_col") VALUES (2, -922337203685477);
INSERT INTO "number_to_numeric_table" ("id", "number_col") VALUES (3, 0);
INSERT INTO "number_to_numeric_table" ("id", "number_col") VALUES (4, 922337203685476);
INSERT INTO "number_to_numeric_table" ("id", "number_col") VALUES (5, -922337203685476);
INSERT INTO "number_to_numeric_table" ("id", "number_col") VALUES (6, NULL);
CREATE TABLE "number_to_string_table" ("id" NUMBER PRIMARY KEY, "number_col" NUMBER);
INSERT INTO "number_to_string_table" ("id", "number_col") VALUES (1, 922337203685477);
INSERT INTO "number_to_string_table" ("id", "number_col") VALUES (2, -922337203685477);
INSERT INTO "number_to_string_table" ("id", "number_col") VALUES (3, 0);
INSERT INTO "number_to_string_table" ("id", "number_col") VALUES (4, 922337203685476);
INSERT INTO "number_to_string_table" ("id", "number_col") VALUES (5, -922337203685476);
INSERT INTO "number_to_string_table" ("id", "number_col") VALUES (6, NULL);
CREATE TABLE "number_to_int64_table" ("id" NUMBER PRIMARY KEY, "number_col" NUMBER);
INSERT INTO "number_to_int64_table" ("id", "number_col") VALUES (1, 922337203685477);
INSERT INTO "number_to_int64_table" ("id", "number_col") VALUES (2, -922337203685477);
INSERT INTO "number_to_int64_table" ("id", "number_col") VALUES (3, 0);
INSERT INTO "number_to_int64_table" ("id", "number_col") VALUES (4, 922337203685476);
INSERT INTO "number_to_int64_table" ("id", "number_col") VALUES (5, -922337203685476);
INSERT INTO "number_to_int64_table" ("id", "number_col") VALUES (6, NULL);
CREATE TABLE "numeric_table" ("id" NUMBER PRIMARY KEY, "numeric_col" NUMERIC);
INSERT INTO "numeric_table" ("id", "numeric_col") VALUES (1, 922337203685477);
INSERT INTO "numeric_table" ("id", "numeric_col") VALUES (2, -922337203685477);
INSERT INTO "numeric_table" ("id", "numeric_col") VALUES (3, 0);
INSERT INTO "numeric_table" ("id", "numeric_col") VALUES (4, 922337203685476);
INSERT INTO "numeric_table" ("id", "numeric_col") VALUES (5, -922337203685476);
INSERT INTO "numeric_table" ("id", "numeric_col") VALUES (6, NULL);
CREATE TABLE "numeric_to_float64_table" ("id" NUMBER PRIMARY KEY, "numeric_col" NUMERIC);
INSERT INTO "numeric_to_float64_table" ("id", "numeric_col") VALUES (1, 922337203685477);
INSERT INTO "numeric_to_float64_table" ("id", "numeric_col") VALUES (2, -922337203685477);
INSERT INTO "numeric_to_float64_table" ("id", "numeric_col") VALUES (3, 0);
INSERT INTO "numeric_to_float64_table" ("id", "numeric_col") VALUES (4, 922337203685476);
INSERT INTO "numeric_to_float64_table" ("id", "numeric_col") VALUES (5, -922337203685476);
INSERT INTO "numeric_to_float64_table" ("id", "numeric_col") VALUES (6, NULL);
CREATE TABLE "numeric_to_string_table" ("id" NUMBER PRIMARY KEY, "numeric_col" NUMERIC);
INSERT INTO "numeric_to_string_table" ("id", "numeric_col") VALUES (1, 922337203685477);
INSERT INTO "numeric_to_string_table" ("id", "numeric_col") VALUES (2, -922337203685477);
INSERT INTO "numeric_to_string_table" ("id", "numeric_col") VALUES (3, 0);
INSERT INTO "numeric_to_string_table" ("id", "numeric_col") VALUES (4, 922337203685476);
INSERT INTO "numeric_to_string_table" ("id", "numeric_col") VALUES (5, -922337203685476);
INSERT INTO "numeric_to_string_table" ("id", "numeric_col") VALUES (6, NULL);
CREATE TABLE "numeric_to_int64_table" ("id" NUMBER PRIMARY KEY, "numeric_col" NUMERIC);
INSERT INTO "numeric_to_int64_table" ("id", "numeric_col") VALUES (1, 922337203685477);
INSERT INTO "numeric_to_int64_table" ("id", "numeric_col") VALUES (2, -922337203685477);
INSERT INTO "numeric_to_int64_table" ("id", "numeric_col") VALUES (3, 0);
INSERT INTO "numeric_to_int64_table" ("id", "numeric_col") VALUES (4, 922337203685476);
INSERT INTO "numeric_to_int64_table" ("id", "numeric_col") VALUES (5, -922337203685476);
INSERT INTO "numeric_to_int64_table" ("id", "numeric_col") VALUES (6, NULL);
CREATE TABLE "decimal_table" ("id" NUMBER PRIMARY KEY, "decimal_col" DECIMAL);
INSERT INTO "decimal_table" ("id", "decimal_col") VALUES (1, 922337203685477);
INSERT INTO "decimal_table" ("id", "decimal_col") VALUES (2, -922337203685477);
INSERT INTO "decimal_table" ("id", "decimal_col") VALUES (3, 0);
INSERT INTO "decimal_table" ("id", "decimal_col") VALUES (4, 922337203685476);
INSERT INTO "decimal_table" ("id", "decimal_col") VALUES (5, -922337203685476);
INSERT INTO "decimal_table" ("id", "decimal_col") VALUES (6, NULL);
CREATE TABLE "decimal_to_float64_table" ("id" NUMBER PRIMARY KEY, "decimal_col" DECIMAL);
INSERT INTO "decimal_to_float64_table" ("id", "decimal_col") VALUES (1, 922337203685477);
INSERT INTO "decimal_to_float64_table" ("id", "decimal_col") VALUES (2, -922337203685477);
INSERT INTO "decimal_to_float64_table" ("id", "decimal_col") VALUES (3, 0);
INSERT INTO "decimal_to_float64_table" ("id", "decimal_col") VALUES (4, 922337203685476);
INSERT INTO "decimal_to_float64_table" ("id", "decimal_col") VALUES (5, -922337203685476);
INSERT INTO "decimal_to_float64_table" ("id", "decimal_col") VALUES (6, NULL);
CREATE TABLE "decimal_to_string_table" ("id" NUMBER PRIMARY KEY, "decimal_col" DECIMAL);
INSERT INTO "decimal_to_string_table" ("id", "decimal_col") VALUES (1, 922337203685477);
INSERT INTO "decimal_to_string_table" ("id", "decimal_col") VALUES (2, -922337203685477);
INSERT INTO "decimal_to_string_table" ("id", "decimal_col") VALUES (3, 0);
INSERT INTO "decimal_to_string_table" ("id", "decimal_col") VALUES (4, 922337203685476);
INSERT INTO "decimal_to_string_table" ("id", "decimal_col") VALUES (5, -922337203685476);
INSERT INTO "decimal_to_string_table" ("id", "decimal_col") VALUES (6, NULL);
CREATE TABLE "decimal_to_int64_table" ("id" NUMBER PRIMARY KEY, "decimal_col" DECIMAL);
INSERT INTO "decimal_to_int64_table" ("id", "decimal_col") VALUES (1, 922337203685477);
INSERT INTO "decimal_to_int64_table" ("id", "decimal_col") VALUES (2, -922337203685477);
INSERT INTO "decimal_to_int64_table" ("id", "decimal_col") VALUES (3, 0);
INSERT INTO "decimal_to_int64_table" ("id", "decimal_col") VALUES (4, 922337203685476);
INSERT INTO "decimal_to_int64_table" ("id", "decimal_col") VALUES (5, -922337203685476);
INSERT INTO "decimal_to_int64_table" ("id", "decimal_col") VALUES (6, NULL);
CREATE TABLE "dec_table" ("id" NUMBER PRIMARY KEY, "dec_col" DEC);
INSERT INTO "dec_table" ("id", "dec_col") VALUES (1, 922337203685477);
INSERT INTO "dec_table" ("id", "dec_col") VALUES (2, -922337203685477);
INSERT INTO "dec_table" ("id", "dec_col") VALUES (3, 0);
INSERT INTO "dec_table" ("id", "dec_col") VALUES (4, 922337203685476);
INSERT INTO "dec_table" ("id", "dec_col") VALUES (5, -922337203685476);
INSERT INTO "dec_table" ("id", "dec_col") VALUES (6, NULL);
CREATE TABLE "dec_to_float64_table" ("id" NUMBER PRIMARY KEY, "dec_col" DEC);
INSERT INTO "dec_to_float64_table" ("id", "dec_col") VALUES (1, 922337203685477);
INSERT INTO "dec_to_float64_table" ("id", "dec_col") VALUES (2, -922337203685477);
INSERT INTO "dec_to_float64_table" ("id", "dec_col") VALUES (3, 0);
INSERT INTO "dec_to_float64_table" ("id", "dec_col") VALUES (4, 922337203685476);
INSERT INTO "dec_to_float64_table" ("id", "dec_col") VALUES (5, -922337203685476);
INSERT INTO "dec_to_float64_table" ("id", "dec_col") VALUES (6, NULL);
CREATE TABLE "dec_to_string_table" ("id" NUMBER PRIMARY KEY, "dec_col" DEC);
INSERT INTO "dec_to_string_table" ("id", "dec_col") VALUES (1, 922337203685477);
INSERT INTO "dec_to_string_table" ("id", "dec_col") VALUES (2, -922337203685477);
INSERT INTO "dec_to_string_table" ("id", "dec_col") VALUES (3, 0);
INSERT INTO "dec_to_string_table" ("id", "dec_col") VALUES (4, 922337203685476);
INSERT INTO "dec_to_string_table" ("id", "dec_col") VALUES (5, -922337203685476);
INSERT INTO "dec_to_string_table" ("id", "dec_col") VALUES (6, NULL);
CREATE TABLE "dec_to_int64_table" ("id" NUMBER PRIMARY KEY, "dec_col" DEC);
INSERT INTO "dec_to_int64_table" ("id", "dec_col") VALUES (1, 922337203685477);
INSERT INTO "dec_to_int64_table" ("id", "dec_col") VALUES (2, -922337203685477);
INSERT INTO "dec_to_int64_table" ("id", "dec_col") VALUES (3, 0);
INSERT INTO "dec_to_int64_table" ("id", "dec_col") VALUES (4, 922337203685476);
INSERT INTO "dec_to_int64_table" ("id", "dec_col") VALUES (5, -922337203685476);
INSERT INTO "dec_to_int64_table" ("id", "dec_col") VALUES (6, NULL);
CREATE TABLE "float_table" ("id" NUMBER PRIMARY KEY, "float_col" FLOAT);
INSERT INTO "float_table" ("id", "float_col") VALUES (1, 922337203685477);
INSERT INTO "float_table" ("id", "float_col") VALUES (2, -922337203685477);
INSERT INTO "float_table" ("id", "float_col") VALUES (3, 0);
INSERT INTO "float_table" ("id", "float_col") VALUES (4, NULL);
INSERT INTO "float_table" ("id", "float_col") VALUES (5, 99999999.99);
INSERT INTO "float_table" ("id", "float_col") VALUES (6, -99999999.99);
INSERT INTO "float_table" ("id", "float_col") VALUES (7, 0.0);
INSERT INTO "float_table" ("id", "float_col") VALUES (8, 99999999.99);
CREATE TABLE "float_to_numeric_table" ("id" NUMBER PRIMARY KEY, "float_col" FLOAT);
INSERT INTO "float_to_numeric_table" ("id", "float_col") VALUES (1, 922337203685477);
INSERT INTO "float_to_numeric_table" ("id", "float_col") VALUES (2, -922337203685477);
INSERT INTO "float_to_numeric_table" ("id", "float_col") VALUES (3, 0);
INSERT INTO "float_to_numeric_table" ("id", "float_col") VALUES (4, NULL);
INSERT INTO "float_to_numeric_table" ("id", "float_col") VALUES (5, 99999999.99);
INSERT INTO "float_to_numeric_table" ("id", "float_col") VALUES (6, -99999999.99);
INSERT INTO "float_to_numeric_table" ("id", "float_col") VALUES (7, 0.0);
INSERT INTO "float_to_numeric_table" ("id", "float_col") VALUES (8, 99999999.99);
CREATE TABLE "float_to_string_table" ("id" NUMBER PRIMARY KEY, "float_col" FLOAT);
INSERT INTO "float_to_string_table" ("id", "float_col") VALUES (1, 922337203685477);
INSERT INTO "float_to_string_table" ("id", "float_col") VALUES (2, -922337203685477);
INSERT INTO "float_to_string_table" ("id", "float_col") VALUES (3, 0);
INSERT INTO "float_to_string_table" ("id", "float_col") VALUES (4, NULL);
INSERT INTO "float_to_string_table" ("id", "float_col") VALUES (5, 99999999.99);
INSERT INTO "float_to_string_table" ("id", "float_col") VALUES (6, -99999999.99);
INSERT INTO "float_to_string_table" ("id", "float_col") VALUES (7, 0.0);
INSERT INTO "float_to_string_table" ("id", "float_col") VALUES (8, 99999999.99);
CREATE TABLE "float_to_int64_table" ("id" NUMBER PRIMARY KEY, "float_col" FLOAT);
INSERT INTO "float_to_int64_table" ("id", "float_col") VALUES (1, 922337203685477);
INSERT INTO "float_to_int64_table" ("id", "float_col") VALUES (2, -922337203685477);
INSERT INTO "float_to_int64_table" ("id", "float_col") VALUES (3, 0);
INSERT INTO "float_to_int64_table" ("id", "float_col") VALUES (4, NULL);
INSERT INTO "float_to_int64_table" ("id", "float_col") VALUES (5, 99999999.99);
INSERT INTO "float_to_int64_table" ("id", "float_col") VALUES (6, -99999999.99);
INSERT INTO "float_to_int64_table" ("id", "float_col") VALUES (7, 0.0);
INSERT INTO "float_to_int64_table" ("id", "float_col") VALUES (8, 99999999.99);
CREATE TABLE "double_precision_table" ("id" NUMBER PRIMARY KEY, "double_precision_col" DOUBLE PRECISION);
INSERT INTO "double_precision_table" ("id", "double_precision_col") VALUES (1, 922337203685477);
INSERT INTO "double_precision_table" ("id", "double_precision_col") VALUES (2, -922337203685477);
INSERT INTO "double_precision_table" ("id", "double_precision_col") VALUES (3, 0);
INSERT INTO "double_precision_table" ("id", "double_precision_col") VALUES (4, NULL);
INSERT INTO "double_precision_table" ("id", "double_precision_col") VALUES (5, 99999999.99);
INSERT INTO "double_precision_table" ("id", "double_precision_col") VALUES (6, -99999999.99);
INSERT INTO "double_precision_table" ("id", "double_precision_col") VALUES (7, 0.0);
INSERT INTO "double_precision_table" ("id", "double_precision_col") VALUES (8, 99999999.99);
CREATE TABLE "double_precision_to_numeric_table" ("id" NUMBER PRIMARY KEY, "double_precision_col" DOUBLE PRECISION);
INSERT INTO "double_precision_to_numeric_table" ("id", "double_precision_col") VALUES (1, 922337203685477);
INSERT INTO "double_precision_to_numeric_table" ("id", "double_precision_col") VALUES (2, -922337203685477);
INSERT INTO "double_precision_to_numeric_table" ("id", "double_precision_col") VALUES (3, 0);
INSERT INTO "double_precision_to_numeric_table" ("id", "double_precision_col") VALUES (4, NULL);
INSERT INTO "double_precision_to_numeric_table" ("id", "double_precision_col") VALUES (5, 99999999.99);
INSERT INTO "double_precision_to_numeric_table" ("id", "double_precision_col") VALUES (6, -99999999.99);
INSERT INTO "double_precision_to_numeric_table" ("id", "double_precision_col") VALUES (7, 0.0);
INSERT INTO "double_precision_to_numeric_table" ("id", "double_precision_col") VALUES (8, 99999999.99);
CREATE TABLE "double_precision_to_string_table" ("id" NUMBER PRIMARY KEY, "double_precision_col" DOUBLE PRECISION);
INSERT INTO "double_precision_to_string_table" ("id", "double_precision_col") VALUES (1, 922337203685477);
INSERT INTO "double_precision_to_string_table" ("id", "double_precision_col") VALUES (2, -922337203685477);
INSERT INTO "double_precision_to_string_table" ("id", "double_precision_col") VALUES (3, 0);
INSERT INTO "double_precision_to_string_table" ("id", "double_precision_col") VALUES (4, NULL);
INSERT INTO "double_precision_to_string_table" ("id", "double_precision_col") VALUES (5, 99999999.99);
INSERT INTO "double_precision_to_string_table" ("id", "double_precision_col") VALUES (6, -99999999.99);
INSERT INTO "double_precision_to_string_table" ("id", "double_precision_col") VALUES (7, 0.0);
INSERT INTO "double_precision_to_string_table" ("id", "double_precision_col") VALUES (8, 99999999.99);
CREATE TABLE "double_precision_to_int64_table" ("id" NUMBER PRIMARY KEY, "double_precision_col" DOUBLE PRECISION);
INSERT INTO "double_precision_to_int64_table" ("id", "double_precision_col") VALUES (1, 922337203685477);
INSERT INTO "double_precision_to_int64_table" ("id", "double_precision_col") VALUES (2, -922337203685477);
INSERT INTO "double_precision_to_int64_table" ("id", "double_precision_col") VALUES (3, 0);
INSERT INTO "double_precision_to_int64_table" ("id", "double_precision_col") VALUES (4, NULL);
INSERT INTO "double_precision_to_int64_table" ("id", "double_precision_col") VALUES (5, 99999999.99);
INSERT INTO "double_precision_to_int64_table" ("id", "double_precision_col") VALUES (6, -99999999.99);
INSERT INTO "double_precision_to_int64_table" ("id", "double_precision_col") VALUES (7, 0.0);
INSERT INTO "double_precision_to_int64_table" ("id", "double_precision_col") VALUES (8, 99999999.99);
CREATE TABLE "real_table" ("id" NUMBER PRIMARY KEY, "real_col" REAL);
INSERT INTO "real_table" ("id", "real_col") VALUES (1, 922337203685477);
INSERT INTO "real_table" ("id", "real_col") VALUES (2, -922337203685477);
INSERT INTO "real_table" ("id", "real_col") VALUES (3, 0);
INSERT INTO "real_table" ("id", "real_col") VALUES (4, NULL);
INSERT INTO "real_table" ("id", "real_col") VALUES (5, 99999999.99);
INSERT INTO "real_table" ("id", "real_col") VALUES (6, -99999999.99);
INSERT INTO "real_table" ("id", "real_col") VALUES (7, 0.0);
INSERT INTO "real_table" ("id", "real_col") VALUES (8, 99999999.99);
CREATE TABLE "real_to_numeric_table" ("id" NUMBER PRIMARY KEY, "real_col" REAL);
INSERT INTO "real_to_numeric_table" ("id", "real_col") VALUES (1, 922337203685477);
INSERT INTO "real_to_numeric_table" ("id", "real_col") VALUES (2, -922337203685477);
INSERT INTO "real_to_numeric_table" ("id", "real_col") VALUES (3, 0);
INSERT INTO "real_to_numeric_table" ("id", "real_col") VALUES (4, NULL);
INSERT INTO "real_to_numeric_table" ("id", "real_col") VALUES (5, 99999999.99);
INSERT INTO "real_to_numeric_table" ("id", "real_col") VALUES (6, -99999999.99);
INSERT INTO "real_to_numeric_table" ("id", "real_col") VALUES (7, 0.0);
INSERT INTO "real_to_numeric_table" ("id", "real_col") VALUES (8, 99999999.99);
CREATE TABLE "real_to_string_table" ("id" NUMBER PRIMARY KEY, "real_col" REAL);
INSERT INTO "real_to_string_table" ("id", "real_col") VALUES (1, 922337203685477);
INSERT INTO "real_to_string_table" ("id", "real_col") VALUES (2, -922337203685477);
INSERT INTO "real_to_string_table" ("id", "real_col") VALUES (3, 0);
INSERT INTO "real_to_string_table" ("id", "real_col") VALUES (4, NULL);
INSERT INTO "real_to_string_table" ("id", "real_col") VALUES (5, 99999999.99);
INSERT INTO "real_to_string_table" ("id", "real_col") VALUES (6, -99999999.99);
INSERT INTO "real_to_string_table" ("id", "real_col") VALUES (7, 0.0);
INSERT INTO "real_to_string_table" ("id", "real_col") VALUES (8, 99999999.99);
CREATE TABLE "real_to_int64_table" ("id" NUMBER PRIMARY KEY, "real_col" REAL);
INSERT INTO "real_to_int64_table" ("id", "real_col") VALUES (1, 922337203685477);
INSERT INTO "real_to_int64_table" ("id", "real_col") VALUES (2, -922337203685477);
INSERT INTO "real_to_int64_table" ("id", "real_col") VALUES (3, 0);
INSERT INTO "real_to_int64_table" ("id", "real_col") VALUES (4, NULL);
INSERT INTO "real_to_int64_table" ("id", "real_col") VALUES (5, 99999999.99);
INSERT INTO "real_to_int64_table" ("id", "real_col") VALUES (6, -99999999.99);
INSERT INTO "real_to_int64_table" ("id", "real_col") VALUES (7, 0.0);
INSERT INTO "real_to_int64_table" ("id", "real_col") VALUES (8, 99999999.99);
CREATE TABLE "binary_float_table" ("id" NUMBER PRIMARY KEY, "binary_float_col" BINARY_FLOAT);
INSERT INTO "binary_float_table" ("id", "binary_float_col") VALUES (1, 922337203685477);
INSERT INTO "binary_float_table" ("id", "binary_float_col") VALUES (2, -922337203685477);
INSERT INTO "binary_float_table" ("id", "binary_float_col") VALUES (3, 0);
INSERT INTO "binary_float_table" ("id", "binary_float_col") VALUES (4, NULL);
INSERT INTO "binary_float_table" ("id", "binary_float_col") VALUES (5, 3.40282e+38);
INSERT INTO "binary_float_table" ("id", "binary_float_col") VALUES (6, -3.40282e+38);
INSERT INTO "binary_float_table" ("id", "binary_float_col") VALUES (7, 0.0);
INSERT INTO "binary_float_table" ("id", "binary_float_col") VALUES (8, 99999999.99);
CREATE TABLE "binary_float_to_float64_table" ("id" NUMBER PRIMARY KEY, "binary_float_col" BINARY_FLOAT);
INSERT INTO "binary_float_to_float64_table" ("id", "binary_float_col") VALUES (1, 922337203685477);
INSERT INTO "binary_float_to_float64_table" ("id", "binary_float_col") VALUES (2, -922337203685477);
INSERT INTO "binary_float_to_float64_table" ("id", "binary_float_col") VALUES (3, 0);
INSERT INTO "binary_float_to_float64_table" ("id", "binary_float_col") VALUES (4, NULL);
INSERT INTO "binary_float_to_float64_table" ("id", "binary_float_col") VALUES (5, 3.40282e+38);
INSERT INTO "binary_float_to_float64_table" ("id", "binary_float_col") VALUES (6, -3.40282e+38);
INSERT INTO "binary_float_to_float64_table" ("id", "binary_float_col") VALUES (7, 0.0);
INSERT INTO "binary_float_to_float64_table" ("id", "binary_float_col") VALUES (8, 99999999.99);
CREATE TABLE "binary_float_to_string_table" ("id" NUMBER PRIMARY KEY, "binary_float_col" BINARY_FLOAT);
INSERT INTO "binary_float_to_string_table" ("id", "binary_float_col") VALUES (1, 922337203685477);
INSERT INTO "binary_float_to_string_table" ("id", "binary_float_col") VALUES (2, -922337203685477);
INSERT INTO "binary_float_to_string_table" ("id", "binary_float_col") VALUES (3, 0);
INSERT INTO "binary_float_to_string_table" ("id", "binary_float_col") VALUES (4, NULL);
INSERT INTO "binary_float_to_string_table" ("id", "binary_float_col") VALUES (5, 3.40282e+38);
INSERT INTO "binary_float_to_string_table" ("id", "binary_float_col") VALUES (6, -3.40282e+38);
INSERT INTO "binary_float_to_string_table" ("id", "binary_float_col") VALUES (7, 0.0);
INSERT INTO "binary_float_to_string_table" ("id", "binary_float_col") VALUES (8, 99999999.99);
CREATE TABLE "binary_float_to_numeric_table" ("id" NUMBER PRIMARY KEY, "binary_float_col" BINARY_FLOAT);
INSERT INTO "binary_float_to_numeric_table" ("id", "binary_float_col") VALUES (1, 922337203685477);
INSERT INTO "binary_float_to_numeric_table" ("id", "binary_float_col") VALUES (2, -922337203685477);
INSERT INTO "binary_float_to_numeric_table" ("id", "binary_float_col") VALUES (3, 0);
INSERT INTO "binary_float_to_numeric_table" ("id", "binary_float_col") VALUES (4, NULL);
INSERT INTO "binary_float_to_numeric_table" ("id", "binary_float_col") VALUES (5, 3.40282e+38);
INSERT INTO "binary_float_to_numeric_table" ("id", "binary_float_col") VALUES (6, -3.40282e+38);
INSERT INTO "binary_float_to_numeric_table" ("id", "binary_float_col") VALUES (7, 0.0);
INSERT INTO "binary_float_to_numeric_table" ("id", "binary_float_col") VALUES (8, 99999999.99);
CREATE TABLE "binary_double_table" ("id" NUMBER PRIMARY KEY, "binary_double_col" BINARY_DOUBLE);
INSERT INTO "binary_double_table" ("id", "binary_double_col") VALUES (1, 922337203685477);
INSERT INTO "binary_double_table" ("id", "binary_double_col") VALUES (2, -922337203685477);
INSERT INTO "binary_double_table" ("id", "binary_double_col") VALUES (3, 0);
INSERT INTO "binary_double_table" ("id", "binary_double_col") VALUES (4, NULL);
INSERT INTO "binary_double_table" ("id", "binary_double_col") VALUES (5, 99999999.99);
INSERT INTO "binary_double_table" ("id", "binary_double_col") VALUES (6, -99999999.99);
INSERT INTO "binary_double_table" ("id", "binary_double_col") VALUES (7, 0.0);
INSERT INTO "binary_double_table" ("id", "binary_double_col") VALUES (8, 99999999.99);
CREATE TABLE "binary_double_to_string_table" ("id" NUMBER PRIMARY KEY, "binary_double_col" BINARY_DOUBLE);
INSERT INTO "binary_double_to_string_table" ("id", "binary_double_col") VALUES (1, 922337203685477);
INSERT INTO "binary_double_to_string_table" ("id", "binary_double_col") VALUES (2, -922337203685477);
INSERT INTO "binary_double_to_string_table" ("id", "binary_double_col") VALUES (3, 0);
INSERT INTO "binary_double_to_string_table" ("id", "binary_double_col") VALUES (4, NULL);
INSERT INTO "binary_double_to_string_table" ("id", "binary_double_col") VALUES (5, 99999999.99);
INSERT INTO "binary_double_to_string_table" ("id", "binary_double_col") VALUES (6, -99999999.99);
INSERT INTO "binary_double_to_string_table" ("id", "binary_double_col") VALUES (7, 0.0);
INSERT INTO "binary_double_to_string_table" ("id", "binary_double_col") VALUES (8, 99999999.99);
CREATE TABLE "binary_double_to_numeric_table" ("id" NUMBER PRIMARY KEY, "binary_double_col" BINARY_DOUBLE);
INSERT INTO "binary_double_to_numeric_table" ("id", "binary_double_col") VALUES (1, 922337203685477);
INSERT INTO "binary_double_to_numeric_table" ("id", "binary_double_col") VALUES (2, -922337203685477);
INSERT INTO "binary_double_to_numeric_table" ("id", "binary_double_col") VALUES (3, 0);
INSERT INTO "binary_double_to_numeric_table" ("id", "binary_double_col") VALUES (4, NULL);
INSERT INTO "binary_double_to_numeric_table" ("id", "binary_double_col") VALUES (5, 99999999.99);
INSERT INTO "binary_double_to_numeric_table" ("id", "binary_double_col") VALUES (6, -99999999.99);
INSERT INTO "binary_double_to_numeric_table" ("id", "binary_double_col") VALUES (7, 0.0);
INSERT INTO "binary_double_to_numeric_table" ("id", "binary_double_col") VALUES (8, 99999999.99);
CREATE TABLE "integer_table" ("id" NUMBER PRIMARY KEY, "integer_col" INTEGER);
INSERT INTO "integer_table" ("id", "integer_col") VALUES (1, 922337203685477);
INSERT INTO "integer_table" ("id", "integer_col") VALUES (2, -922337203685477);
INSERT INTO "integer_table" ("id", "integer_col") VALUES (3, 0);
INSERT INTO "integer_table" ("id", "integer_col") VALUES (4, NULL);
INSERT INTO "integer_table" ("id", "integer_col") VALUES (5, 922337203685476);
CREATE TABLE "integer_to_numeric_table" ("id" NUMBER PRIMARY KEY, "integer_col" INTEGER);
INSERT INTO "integer_to_numeric_table" ("id", "integer_col") VALUES (1, 922337203685477);
INSERT INTO "integer_to_numeric_table" ("id", "integer_col") VALUES (2, -922337203685477);
INSERT INTO "integer_to_numeric_table" ("id", "integer_col") VALUES (3, 0);
INSERT INTO "integer_to_numeric_table" ("id", "integer_col") VALUES (4, NULL);
INSERT INTO "integer_to_numeric_table" ("id", "integer_col") VALUES (5, 922337203685476);
CREATE TABLE "integer_to_string_table" ("id" NUMBER PRIMARY KEY, "integer_col" INTEGER);
INSERT INTO "integer_to_string_table" ("id", "integer_col") VALUES (1, 922337203685477);
INSERT INTO "integer_to_string_table" ("id", "integer_col") VALUES (2, -922337203685477);
INSERT INTO "integer_to_string_table" ("id", "integer_col") VALUES (3, 0);
INSERT INTO "integer_to_string_table" ("id", "integer_col") VALUES (4, NULL);
INSERT INTO "integer_to_string_table" ("id", "integer_col") VALUES (5, 922337203685476);
CREATE TABLE "integer_to_float64_table" ("id" NUMBER PRIMARY KEY, "integer_col" INTEGER);
INSERT INTO "integer_to_float64_table" ("id", "integer_col") VALUES (1, 922337203685477);
INSERT INTO "integer_to_float64_table" ("id", "integer_col") VALUES (2, -922337203685477);
INSERT INTO "integer_to_float64_table" ("id", "integer_col") VALUES (3, 0);
INSERT INTO "integer_to_float64_table" ("id", "integer_col") VALUES (4, NULL);
INSERT INTO "integer_to_float64_table" ("id", "integer_col") VALUES (5, 922337203685476);
CREATE TABLE "integer_pk_table" ("integer_pk_col" INTEGER PRIMARY KEY);
INSERT INTO "integer_pk_table" ("integer_pk_col") VALUES (922337203685476);
INSERT INTO "integer_pk_table" ("integer_pk_col") VALUES (-922337203685477);
INSERT INTO "integer_pk_table" ("integer_pk_col") VALUES (0);
CREATE TABLE "int_table" ("id" NUMBER PRIMARY KEY, "int_col" INT);
INSERT INTO "int_table" ("id", "int_col") VALUES (1, 922337203685477);
INSERT INTO "int_table" ("id", "int_col") VALUES (2, -922337203685477);
INSERT INTO "int_table" ("id", "int_col") VALUES (3, 0);
INSERT INTO "int_table" ("id", "int_col") VALUES (4, NULL);
INSERT INTO "int_table" ("id", "int_col") VALUES (5, 922337203685476);
CREATE TABLE "int_to_numeric_table" ("id" NUMBER PRIMARY KEY, "int_col" INT);
INSERT INTO "int_to_numeric_table" ("id", "int_col") VALUES (1, 922337203685477);
INSERT INTO "int_to_numeric_table" ("id", "int_col") VALUES (2, -922337203685477);
INSERT INTO "int_to_numeric_table" ("id", "int_col") VALUES (3, 0);
INSERT INTO "int_to_numeric_table" ("id", "int_col") VALUES (4, NULL);
INSERT INTO "int_to_numeric_table" ("id", "int_col") VALUES (5, 922337203685476);
CREATE TABLE "int_to_string_table" ("id" NUMBER PRIMARY KEY, "int_col" INT);
INSERT INTO "int_to_string_table" ("id", "int_col") VALUES (1, 922337203685477);
INSERT INTO "int_to_string_table" ("id", "int_col") VALUES (2, -922337203685477);
INSERT INTO "int_to_string_table" ("id", "int_col") VALUES (3, 0);
INSERT INTO "int_to_string_table" ("id", "int_col") VALUES (4, NULL);
INSERT INTO "int_to_string_table" ("id", "int_col") VALUES (5, 922337203685476);
CREATE TABLE "int_to_float64_table" ("id" NUMBER PRIMARY KEY, "int_col" INT);
INSERT INTO "int_to_float64_table" ("id", "int_col") VALUES (1, 922337203685477);
INSERT INTO "int_to_float64_table" ("id", "int_col") VALUES (2, -922337203685477);
INSERT INTO "int_to_float64_table" ("id", "int_col") VALUES (3, 0);
INSERT INTO "int_to_float64_table" ("id", "int_col") VALUES (4, NULL);
INSERT INTO "int_to_float64_table" ("id", "int_col") VALUES (5, 922337203685476);
CREATE TABLE "int_pk_table" ("int_pk_col" INT PRIMARY KEY);
INSERT INTO "int_pk_table" ("int_pk_col") VALUES (922337203685476);
INSERT INTO "int_pk_table" ("int_pk_col") VALUES (-922337203685477);
INSERT INTO "int_pk_table" ("int_pk_col") VALUES (0);
CREATE TABLE "smallint_table" ("id" NUMBER PRIMARY KEY, "smallint_col" SMALLINT);
INSERT INTO "smallint_table" ("id", "smallint_col") VALUES (1, 922337203685477);
INSERT INTO "smallint_table" ("id", "smallint_col") VALUES (2, -922337203685477);
INSERT INTO "smallint_table" ("id", "smallint_col") VALUES (3, 0);
INSERT INTO "smallint_table" ("id", "smallint_col") VALUES (4, NULL);
INSERT INTO "smallint_table" ("id", "smallint_col") VALUES (5, 922337203685476);
CREATE TABLE "smallint_to_numeric_table" ("id" NUMBER PRIMARY KEY, "smallint_col" SMALLINT);
INSERT INTO "smallint_to_numeric_table" ("id", "smallint_col") VALUES (1, 922337203685477);
INSERT INTO "smallint_to_numeric_table" ("id", "smallint_col") VALUES (2, -922337203685477);
INSERT INTO "smallint_to_numeric_table" ("id", "smallint_col") VALUES (3, 0);
INSERT INTO "smallint_to_numeric_table" ("id", "smallint_col") VALUES (4, NULL);
INSERT INTO "smallint_to_numeric_table" ("id", "smallint_col") VALUES (5, 922337203685476);
CREATE TABLE "smallint_to_string_table" ("id" NUMBER PRIMARY KEY, "smallint_col" SMALLINT);
INSERT INTO "smallint_to_string_table" ("id", "smallint_col") VALUES (1, 922337203685477);
INSERT INTO "smallint_to_string_table" ("id", "smallint_col") VALUES (2, -922337203685477);
INSERT INTO "smallint_to_string_table" ("id", "smallint_col") VALUES (3, 0);
INSERT INTO "smallint_to_string_table" ("id", "smallint_col") VALUES (4, NULL);
INSERT INTO "smallint_to_string_table" ("id", "smallint_col") VALUES (5, 922337203685476);
CREATE TABLE "smallint_to_float64_table" ("id" NUMBER PRIMARY KEY, "smallint_col" SMALLINT);
INSERT INTO "smallint_to_float64_table" ("id", "smallint_col") VALUES (1, 922337203685477);
INSERT INTO "smallint_to_float64_table" ("id", "smallint_col") VALUES (2, -922337203685477);
INSERT INTO "smallint_to_float64_table" ("id", "smallint_col") VALUES (3, 0);
INSERT INTO "smallint_to_float64_table" ("id", "smallint_col") VALUES (4, NULL);
INSERT INTO "smallint_to_float64_table" ("id", "smallint_col") VALUES (5, 922337203685476);
CREATE TABLE "smallint_pk_table" ("smallint_pk_col" SMALLINT PRIMARY KEY);
INSERT INTO "smallint_pk_table" ("smallint_pk_col") VALUES (922337203685476);
INSERT INTO "smallint_pk_table" ("smallint_pk_col") VALUES (-922337203685477);
INSERT INTO "smallint_pk_table" ("smallint_pk_col") VALUES (0);
CREATE TABLE "date_table" ("id" NUMBER PRIMARY KEY, "date_col" DATE);
INSERT INTO "date_table" ("id", "date_col") VALUES (1, TIMESTAMP '0001-01-01 00:00:00');
INSERT INTO "date_table" ("id", "date_col") VALUES (2, TIMESTAMP '9999-12-31 23:59:59');
INSERT INTO "date_table" ("id", "date_col") VALUES (3, NULL);
CREATE TABLE "date_to_date_table" ("id" NUMBER PRIMARY KEY, "date_col" DATE);
INSERT INTO "date_to_date_table" ("id", "date_col") VALUES (1, TIMESTAMP '0001-01-01 00:00:00');
INSERT INTO "date_to_date_table" ("id", "date_col") VALUES (2, TIMESTAMP '9999-12-31 23:59:59');
INSERT INTO "date_to_date_table" ("id", "date_col") VALUES (3, NULL);
CREATE TABLE "date_to_string_table" ("id" NUMBER PRIMARY KEY, "date_col" DATE);
INSERT INTO "date_to_string_table" ("id", "date_col") VALUES (1, TIMESTAMP '0001-01-01 00:00:00');
INSERT INTO "date_to_string_table" ("id", "date_col") VALUES (2, TIMESTAMP '9999-12-31 23:59:59');
INSERT INTO "date_to_string_table" ("id", "date_col") VALUES (3, NULL);
CREATE TABLE "date_to_int64_table" ("id" NUMBER PRIMARY KEY, "date_col" DATE);
INSERT INTO "date_to_int64_table" ("id", "date_col") VALUES (1, TIMESTAMP '0001-01-01 00:00:00');
INSERT INTO "date_to_int64_table" ("id", "date_col") VALUES (2, TIMESTAMP '9999-12-31 23:59:59');
INSERT INTO "date_to_int64_table" ("id", "date_col") VALUES (3, NULL);
CREATE TABLE "date_pk_table" ("date_pk_col" DATE PRIMARY KEY);
INSERT INTO "date_pk_table" ("date_pk_col") VALUES (TIMESTAMP '0001-01-01 00:00:00');
INSERT INTO "date_pk_table" ("date_pk_col") VALUES (TIMESTAMP '9999-12-31 23:59:59');
CREATE TABLE "timestamp_table" ("id" NUMBER PRIMARY KEY, "timestamp_col" TIMESTAMP);
INSERT INTO "timestamp_table" ("id", "timestamp_col") VALUES (1, TIMESTAMP '0001-01-01 00:00:00');
INSERT INTO "timestamp_table" ("id", "timestamp_col") VALUES (2, TIMESTAMP '9999-12-31 23:59:59');
INSERT INTO "timestamp_table" ("id", "timestamp_col") VALUES (3, NULL);
CREATE TABLE "timestamp_to_string_table" ("id" NUMBER PRIMARY KEY, "timestamp_col" TIMESTAMP);
INSERT INTO "timestamp_to_string_table" ("id", "timestamp_col") VALUES (1, TIMESTAMP '0001-01-01 00:00:00');
INSERT INTO "timestamp_to_string_table" ("id", "timestamp_col") VALUES (2, TIMESTAMP '9999-12-31 23:59:59');
INSERT INTO "timestamp_to_string_table" ("id", "timestamp_col") VALUES (3, NULL);
CREATE TABLE "timestamp_to_int64_table" ("id" NUMBER PRIMARY KEY, "timestamp_col" TIMESTAMP);
INSERT INTO "timestamp_to_int64_table" ("id", "timestamp_col") VALUES (1, TIMESTAMP '0001-01-01 00:00:00');
INSERT INTO "timestamp_to_int64_table" ("id", "timestamp_col") VALUES (2, TIMESTAMP '9999-12-31 23:59:59');
INSERT INTO "timestamp_to_int64_table" ("id", "timestamp_col") VALUES (3, NULL);
CREATE TABLE "timestamp_pk_table" ("timestamp_pk_col" TIMESTAMP PRIMARY KEY);
INSERT INTO "timestamp_pk_table" ("timestamp_pk_col") VALUES (TIMESTAMP '0001-01-01 00:00:00');
INSERT INTO "timestamp_pk_table" ("timestamp_pk_col") VALUES (TIMESTAMP '9999-12-31 23:59:59');






CREATE TABLE "interval_year_to_month_table" ("id" NUMBER PRIMARY KEY, "interval_year_to_month_col" INTERVAL YEAR TO MONTH);
INSERT INTO "interval_year_to_month_table" ("id", "interval_year_to_month_col") VALUES (1, '+99-11');
INSERT INTO "interval_year_to_month_table" ("id", "interval_year_to_month_col") VALUES (2, '-99-11');
CREATE TABLE "interval_year_to_month_to_bigint_months_table" ("id" NUMBER PRIMARY KEY, "interval_year_to_month_col" INTERVAL YEAR TO MONTH);
INSERT INTO "interval_year_to_month_to_bigint_months_table" ("id", "interval_year_to_month_col") VALUES (1, '+99-11');
INSERT INTO "interval_year_to_month_to_bigint_months_table" ("id", "interval_year_to_month_col") VALUES (2, '-99-11');
CREATE TABLE "interval_year_to_month_to_float64_table" ("id" NUMBER PRIMARY KEY, "interval_year_to_month_col" INTERVAL YEAR TO MONTH);
INSERT INTO "interval_year_to_month_to_float64_table" ("id", "interval_year_to_month_col") VALUES (1, '+99-11');
INSERT INTO "interval_year_to_month_to_float64_table" ("id", "interval_year_to_month_col") VALUES (2, '-99-11');
CREATE TABLE "interval_day_to_second_table" ("id" NUMBER PRIMARY KEY, "interval_day_to_second_col" INTERVAL DAY TO SECOND);
INSERT INTO "interval_day_to_second_table" ("id", "interval_day_to_second_col") VALUES (3, '+99 23:59:59.999999');
CREATE TABLE "interval_day_to_second_to_bigint_millis_table" ("id" NUMBER PRIMARY KEY, "interval_day_to_second_col" INTERVAL DAY TO SECOND);
INSERT INTO "interval_day_to_second_to_bigint_millis_table" ("id", "interval_day_to_second_col") VALUES (3, '+99 23:59:59.999999');
CREATE TABLE "interval_day_to_second_to_float64_table" ("id" NUMBER PRIMARY KEY, "interval_day_to_second_col" INTERVAL DAY TO SECOND);
INSERT INTO "interval_day_to_second_to_float64_table" ("id", "interval_day_to_second_col") VALUES (3, '+99 23:59:59.999999');
CREATE TABLE "raw_table" ("id" NUMBER PRIMARY KEY, "raw_col" RAW(2000));
INSERT INTO "raw_table" ("id", "raw_col") VALUES (1, NULL);
INSERT INTO "raw_table" ("id", "raw_col") VALUES (2, NULL);
INSERT INTO "raw_table" ("id", "raw_col") VALUES (3, UTL_RAW.CAST_TO_RAW('A'));
INSERT INTO "raw_table" ("id", "raw_col") VALUES (4, UTL_RAW.CAST_TO_RAW('DROP TABLE'));
CREATE TABLE "raw_to_bytes_table" ("id" NUMBER PRIMARY KEY, "raw_col" RAW(2000));
INSERT INTO "raw_to_bytes_table" ("id", "raw_col") VALUES (1, NULL);
INSERT INTO "raw_to_bytes_table" ("id", "raw_col") VALUES (2, NULL);
INSERT INTO "raw_to_bytes_table" ("id", "raw_col") VALUES (3, UTL_RAW.CAST_TO_RAW('A'));
INSERT INTO "raw_to_bytes_table" ("id", "raw_col") VALUES (4, UTL_RAW.CAST_TO_RAW('DROP TABLE'));
CREATE TABLE "raw_to_varchar_base64_table" ("id" NUMBER PRIMARY KEY, "raw_col" RAW(2000));
INSERT INTO "raw_to_varchar_base64_table" ("id", "raw_col") VALUES (1, NULL);
INSERT INTO "raw_to_varchar_base64_table" ("id", "raw_col") VALUES (2, NULL);
INSERT INTO "raw_to_varchar_base64_table" ("id", "raw_col") VALUES (3, UTL_RAW.CAST_TO_RAW('A'));
INSERT INTO "raw_to_varchar_base64_table" ("id", "raw_col") VALUES (4, UTL_RAW.CAST_TO_RAW('DROP TABLE'));
CREATE TABLE "long_raw_table" ("id" NUMBER PRIMARY KEY, "long_raw_col" LONG RAW);
INSERT INTO "long_raw_table" ("id", "long_raw_col") VALUES (1, UTL_RAW.CAST_TO_RAW('""'));
INSERT INTO "long_raw_table" ("id", "long_raw_col") VALUES (2, UTL_RAW.CAST_TO_RAW('"A"*100000'));
INSERT INTO "long_raw_table" ("id", "long_raw_col") VALUES (3, UTL_RAW.CAST_TO_RAW('"NULL"'));
CREATE TABLE "long_raw_to_varchar_base64_table" ("id" NUMBER PRIMARY KEY, "long_raw_col" LONG RAW);
INSERT INTO "long_raw_to_varchar_base64_table" ("id", "long_raw_col") VALUES (1, UTL_RAW.CAST_TO_RAW('""'));
INSERT INTO "long_raw_to_varchar_base64_table" ("id", "long_raw_col") VALUES (2, UTL_RAW.CAST_TO_RAW('"A"*100000'));
INSERT INTO "long_raw_to_varchar_base64_table" ("id", "long_raw_col") VALUES (3, UTL_RAW.CAST_TO_RAW('"NULL"'));
CREATE TABLE "blob_table" ("id" NUMBER PRIMARY KEY, "blob_col" BLOB);
INSERT INTO "blob_table" ("id", "blob_col") VALUES (1, UTL_RAW.CAST_TO_RAW('""'));
INSERT INTO "blob_table" ("id", "blob_col") VALUES (2, UTL_RAW.CAST_TO_RAW('"A"*100000'));
INSERT INTO "blob_table" ("id", "blob_col") VALUES (3, NULL);
CREATE TABLE "blob_to_varchar_base64_table" ("id" NUMBER PRIMARY KEY, "blob_col" BLOB);
INSERT INTO "blob_to_varchar_base64_table" ("id", "blob_col") VALUES (1, UTL_RAW.CAST_TO_RAW('""'));
INSERT INTO "blob_to_varchar_base64_table" ("id", "blob_col") VALUES (2, UTL_RAW.CAST_TO_RAW('"A"*100000'));
INSERT INTO "blob_to_varchar_base64_table" ("id", "blob_col") VALUES (3, NULL);
CREATE TABLE "clob_table" ("id" NUMBER PRIMARY KEY, "clob_col" CLOB);
INSERT INTO "clob_table" ("id", "clob_col") VALUES (1, '""');
INSERT INTO "clob_table" ("id", "clob_col") VALUES (2, '"A"*100000');
INSERT INTO "clob_table" ("id", "clob_col") VALUES (3, NULL);
CREATE TABLE "clob_to_bytes_table" ("id" NUMBER PRIMARY KEY, "clob_col" CLOB);
INSERT INTO "clob_to_bytes_table" ("id", "clob_col") VALUES (1, '""');
INSERT INTO "clob_to_bytes_table" ("id", "clob_col") VALUES (2, '"A"*100000');
INSERT INTO "clob_to_bytes_table" ("id", "clob_col") VALUES (3, NULL);
CREATE TABLE "nclob_table" ("id" NUMBER PRIMARY KEY, "nclob_col" NCLOB);
INSERT INTO "nclob_table" ("id", "nclob_col") VALUES (1, '""');
INSERT INTO "nclob_table" ("id", "nclob_col") VALUES (2, '"A"*100000');
INSERT INTO "nclob_table" ("id", "nclob_col") VALUES (3, NULL);
CREATE TABLE "nclob_to_bytes_table" ("id" NUMBER PRIMARY KEY, "nclob_col" NCLOB);
INSERT INTO "nclob_to_bytes_table" ("id", "nclob_col") VALUES (1, '""');
INSERT INTO "nclob_to_bytes_table" ("id", "nclob_col") VALUES (2, '"A"*100000');
INSERT INTO "nclob_to_bytes_table" ("id", "nclob_col") VALUES (3, NULL);
CREATE TABLE "bfile_table" ("id" NUMBER PRIMARY KEY, "bfile_col" BFILE);
INSERT INTO "bfile_table" ("id", "bfile_col") VALUES (1, NULL);
INSERT INTO "bfile_table" ("id", "bfile_col") VALUES (2, NULL);
CREATE TABLE "bfile_to_bytes_table" ("id" NUMBER PRIMARY KEY, "bfile_col" BFILE);
INSERT INTO "bfile_to_bytes_table" ("id", "bfile_col") VALUES (1, NULL);
INSERT INTO "bfile_to_bytes_table" ("id", "bfile_col") VALUES (2, NULL);
CREATE TABLE "bfile_to_varchar_url_table" ("id" NUMBER PRIMARY KEY, "bfile_col" BFILE);
INSERT INTO "bfile_to_varchar_url_table" ("id", "bfile_col") VALUES (1, NULL);
INSERT INTO "bfile_to_varchar_url_table" ("id", "bfile_col") VALUES (2, NULL);
CREATE TABLE "long_table" ("id" NUMBER PRIMARY KEY, "long_col" LONG);
INSERT INTO "long_table" ("id", "long_col") VALUES (1, '""');
INSERT INTO "long_table" ("id", "long_col") VALUES (2, '"A"*100000');
INSERT INTO "long_table" ("id", "long_col") VALUES (3, '"NULL"');
CREATE TABLE "long_to_bytes_table" ("id" NUMBER PRIMARY KEY, "long_col" LONG);
INSERT INTO "long_to_bytes_table" ("id", "long_col") VALUES (1, '""');
INSERT INTO "long_to_bytes_table" ("id", "long_col") VALUES (2, '"A"*100000');
INSERT INTO "long_to_bytes_table" ("id", "long_col") VALUES (3, '"NULL"');
CREATE TABLE "rowid_table" ("id" NUMBER PRIMARY KEY, "rowid_col" ROWID);
INSERT INTO "rowid_table" ("id", "rowid_col") VALUES (1, 'AAAB12AADAAAAwPAAA');
INSERT INTO "rowid_table" ("id", "rowid_col") VALUES (2, NULL);
CREATE TABLE "rowid_to_bytes_table" ("id" NUMBER PRIMARY KEY, "rowid_col" ROWID);
INSERT INTO "rowid_to_bytes_table" ("id", "rowid_col") VALUES (1, 'AAAB12AADAAAAwPAAA');
INSERT INTO "rowid_to_bytes_table" ("id", "rowid_col") VALUES (2, NULL);
CREATE TABLE "rowid_to_int64_table" ("id" NUMBER PRIMARY KEY, "rowid_col" ROWID);
INSERT INTO "rowid_to_int64_table" ("id", "rowid_col") VALUES (1, 'AAAB12AADAAAAwPAAA');
INSERT INTO "rowid_to_int64_table" ("id", "rowid_col") VALUES (2, NULL);
CREATE TABLE "urowid_table" ("id" NUMBER PRIMARY KEY, "urowid_col" UROWID);
INSERT INTO "urowid_table" ("id", "urowid_col") VALUES (1, 'AAAB12AADAAAAwPAAA');
INSERT INTO "urowid_table" ("id", "urowid_col") VALUES (2, NULL);
CREATE TABLE "urowid_to_bytes_table" ("id" NUMBER PRIMARY KEY, "urowid_col" UROWID);
INSERT INTO "urowid_to_bytes_table" ("id", "urowid_col") VALUES (1, 'AAAB12AADAAAAwPAAA');
INSERT INTO "urowid_to_bytes_table" ("id", "urowid_col") VALUES (2, NULL);
CREATE TABLE "urowid_to_int64_table" ("id" NUMBER PRIMARY KEY, "urowid_col" UROWID);
INSERT INTO "urowid_to_int64_table" ("id", "urowid_col") VALUES (1, 'AAAB12AADAAAAwPAAA');
INSERT INTO "urowid_to_int64_table" ("id", "urowid_col") VALUES (2, NULL);















CREATE TABLE "json_table" ("id" NUMBER PRIMARY KEY, "json_col" JSON);
INSERT INTO "json_table" ("id", "json_col") VALUES (1, '{}');
INSERT INTO "json_table" ("id", "json_col") VALUES (2, '[]');
INSERT INTO "json_table" ("id", "json_col") VALUES (3, '{"a": 1}');
CREATE TABLE "json_to_string_table" ("id" NUMBER PRIMARY KEY, "json_col" JSON);
INSERT INTO "json_to_string_table" ("id", "json_col") VALUES (1, '{}');
INSERT INTO "json_to_string_table" ("id", "json_col") VALUES (2, '[]');
INSERT INTO "json_to_string_table" ("id", "json_col") VALUES (3, '{"a": 1}');
CREATE TABLE "json_to_bytes_table" ("id" NUMBER PRIMARY KEY, "json_col" JSON);
INSERT INTO "json_to_bytes_table" ("id", "json_col") VALUES (1, '{}');
INSERT INTO "json_to_bytes_table" ("id", "json_col") VALUES (2, '[]');
INSERT INTO "json_to_bytes_table" ("id", "json_col") VALUES (3, '{"a": 1}');
CREATE TABLE "xmltype_table" ("id" NUMBER PRIMARY KEY, "xmltype_col" XMLType);
INSERT INTO "xmltype_table" ("id", "xmltype_col") VALUES (1, '<xml></xml>');
INSERT INTO "xmltype_table" ("id", "xmltype_col") VALUES (2, '<xml_doc/>');
INSERT INTO "xmltype_table" ("id", "xmltype_col") VALUES (3, NULL);
CREATE TABLE "xmltype_to_bytes_table" ("id" NUMBER PRIMARY KEY, "xmltype_col" XMLType);
INSERT INTO "xmltype_to_bytes_table" ("id", "xmltype_col") VALUES (1, '<xml></xml>');
INSERT INTO "xmltype_to_bytes_table" ("id", "xmltype_col") VALUES (2, '<xml_doc/>');
INSERT INTO "xmltype_to_bytes_table" ("id", "xmltype_col") VALUES (3, NULL);































































































































































CREATE TABLE "timestamp_with_time_zone_table" ("id" INT, "timestamp_with_time_zone_col" TIMESTAMP WITH TIME ZONE, PRIMARY KEY ("id"));
CREATE TABLE "timestamp_with_time_zone_to_string_table" ("id" INT, "timestamp_with_time_zone_to_varchar_col" TIMESTAMP WITH TIME ZONE, PRIMARY KEY ("id"));
CREATE TABLE "timestamp_with_time_zone_to_int64_table" ("id" INT, "timestamp_with_time_zone_to_bigint_col" TIMESTAMP WITH TIME ZONE, PRIMARY KEY ("id"));
CREATE TABLE "timestamp_with_local_time_zone_table" ("id" INT, "timestamp_with_local_time_zone_col" TIMESTAMP WITH LOCAL TIME ZONE, PRIMARY KEY ("id"));
CREATE TABLE "timestamp_with_local_time_zone_to_string_table" ("id" INT, "timestamp_with_local_time_zone_to_varchar_col" TIMESTAMP WITH LOCAL TIME ZONE, PRIMARY KEY ("id"));
CREATE TABLE "timestamp_with_local_time_zone_to_int64_table" ("id" INT, "timestamp_with_local_time_zone_to_bigint_col" TIMESTAMP WITH LOCAL TIME ZONE, PRIMARY KEY ("id"));
INSERT INTO "timestamp_with_time_zone_table" ("id", "timestamp_with_time_zone_col") VALUES (1, TIMESTAMP '0001-01-01 00:00:00');
INSERT INTO "timestamp_with_time_zone_table" ("id", "timestamp_with_time_zone_col") VALUES (2, TIMESTAMP '9999-12-31 23:59:59');
INSERT INTO "timestamp_with_time_zone_table" ("id", "timestamp_with_time_zone_col") VALUES (3, NULL);
INSERT INTO "timestamp_with_time_zone_to_string_table" ("id", "timestamp_with_time_zone_to_varchar_col") VALUES (1, TIMESTAMP '0001-01-01 00:00:00');
INSERT INTO "timestamp_with_time_zone_to_string_table" ("id", "timestamp_with_time_zone_to_varchar_col") VALUES (2, TIMESTAMP '9999-12-31 23:59:59');
INSERT INTO "timestamp_with_time_zone_to_string_table" ("id", "timestamp_with_time_zone_to_varchar_col") VALUES (3, NULL);
INSERT INTO "timestamp_with_time_zone_to_int64_table" ("id", "timestamp_with_time_zone_to_bigint_col") VALUES (1, TIMESTAMP '0001-01-01 00:00:00');
INSERT INTO "timestamp_with_time_zone_to_int64_table" ("id", "timestamp_with_time_zone_to_bigint_col") VALUES (2, TIMESTAMP '9999-12-31 23:59:59');
INSERT INTO "timestamp_with_time_zone_to_int64_table" ("id", "timestamp_with_time_zone_to_bigint_col") VALUES (3, NULL);
INSERT INTO "timestamp_with_local_time_zone_table" ("id", "timestamp_with_local_time_zone_col") VALUES (1, TIMESTAMP '0001-01-01 00:00:00');
INSERT INTO "timestamp_with_local_time_zone_table" ("id", "timestamp_with_local_time_zone_col") VALUES (2, TIMESTAMP '9999-12-31 23:59:59');
INSERT INTO "timestamp_with_local_time_zone_table" ("id", "timestamp_with_local_time_zone_col") VALUES (3, NULL);
INSERT INTO "timestamp_with_local_time_zone_to_string_table" ("id", "timestamp_with_local_time_zone_to_varchar_col") VALUES (1, TIMESTAMP '0001-01-01 00:00:00');
INSERT INTO "timestamp_with_local_time_zone_to_string_table" ("id", "timestamp_with_local_time_zone_to_varchar_col") VALUES (2, TIMESTAMP '9999-12-31 23:59:59');
INSERT INTO "timestamp_with_local_time_zone_to_string_table" ("id", "timestamp_with_local_time_zone_to_varchar_col") VALUES (3, NULL);
INSERT INTO "timestamp_with_local_time_zone_to_int64_table" ("id", "timestamp_with_local_time_zone_to_bigint_col") VALUES (1, TIMESTAMP '0001-01-01 00:00:00');
INSERT INTO "timestamp_with_local_time_zone_to_int64_table" ("id", "timestamp_with_local_time_zone_to_bigint_col") VALUES (2, TIMESTAMP '9999-12-31 23:59:59');
INSERT INTO "timestamp_with_local_time_zone_to_int64_table" ("id", "timestamp_with_local_time_zone_to_bigint_col") VALUES (3, NULL);
