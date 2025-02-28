/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.spanner.migrations.exceptions;

import com.google.cloud.spanner.SpannerException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.SpannerMigrationException.ErrorCode;

/* Helper class to parse SpannerExceptions and convert it to a SpannerMigrationException */
public class SpannerExceptionParser {

  /*
  Error message substrings to be inspected from Spanner exception
   */
  public static final String TABLE_NOT_FOUND = "Table not found";
  public static final String COLUMN_NOT_FOUND = "Column not found in table";
  public static final String UNIQUE_INDEX_VIOLATION = "Unique index violation on index";
  public static final String DATATYPE_MISMATCH = "Invalid value for column";
  public static final String NOT_NULL_VIOLATION = "must not be NULL";
  public static final String WRITE_TO_GENERATED_COLUMN = "Cannot write into generated column";
  public static final String GENERATED_PK_NOT_FULLY_SPECIFIED_1 =
      "For an Update, the value of a generated primary key";
  public static final String GENERATED_PK_NOT_FULLY_SPECIFIED_2 =
      "must be explicitly specified, or else its non-key dependent column";
  public static final String DUPLICATE_COLUMN = "Duplicate column";
  public static final String WRONG_NUMBER_OF_KEY_PARTS = "Wrong number of key parts";
  public static final String CHECK_CONSTRAINT_VIOLATION_1 = "Check constraint";
  public static final String CHECK_CONSTRAINT_VIOLATION_2 = "is violated";
  public static final String CELL_VALUE_TOO_LARGE =
      "Invalid commit request: maximum size for a single column value is";
  public static final String PARENT_NOT_FOUND_1 = "Parent row for row";
  public static final String PARENT_NOT_FOUND_2 = "is missing";
  public static final String FK_VIOLATION_CHILD_INSERT_1 = "Foreign key constraint";
  public static final String FK_VIOLATION_CHILD_INSERT_2 = "Cannot find referenced values in";
  public static final String FK_VIOLATION_PARENT_DELETE =
      "Foreign key constraint violation when deleting or updating referenced row(s)";
  public static final String FK_VIOLATION_PARENT_UPDATE =
      "Integrity constraint violation during DELETE/REPLACE. Found child row";

  public static SpannerMigrationException parse(SpannerException exception) {
    ErrorCode errorCode =
        switch (exception.getErrorCode()) {
          case UNIMPLEMENTED -> ErrorCode.SPANNER_UNIMPLEMENTED;
          case UNAUTHENTICATED -> ErrorCode.SPANNER_UNAUTHENTICATED;
          case PERMISSION_DENIED -> ErrorCode.SPANNER_PERMISSION_DENIED;
          case UNAVAILABLE -> ErrorCode.SPANNER_UNAVAILABLE;
          case OUT_OF_RANGE -> {
            if (containsIgnoreCase(exception.getMessage(), CHECK_CONSTRAINT_VIOLATION_1)
                && containsIgnoreCase(exception.getMessage(), CHECK_CONSTRAINT_VIOLATION_2)) {
              yield ErrorCode.SPANNER_CHECK_CONSTRAINT_VIOLATION;
            } else {
              yield ErrorCode.SPANNER_OUT_OF_RANGE;
            }
          }
          case INVALID_ARGUMENT -> {
            if (containsIgnoreCase(exception.getMessage(), CELL_VALUE_TOO_LARGE)) {
              yield ErrorCode.SPANNER_VALUE_TOO_LARGE;
            } else {
              yield ErrorCode.SPANNER_INVALID_ARGUMENT;
            }
          }
          case NOT_FOUND -> {
            if (containsIgnoreCase(exception.getMessage(), TABLE_NOT_FOUND)) {
              yield ErrorCode.SPANNER_TABLE_NOT_FOUND;
            } else if (containsIgnoreCase(exception.getMessage(), COLUMN_NOT_FOUND)) {
              yield ErrorCode.SPANNER_COLUMN_NOT_FOUND;
            } else if (containsIgnoreCase(exception.getMessage(), PARENT_NOT_FOUND_1)
                && containsIgnoreCase(exception.getMessage(), PARENT_NOT_FOUND_2)) {
              yield ErrorCode.SPANNER_PARENT_NOT_FOUND;
            } else {
              yield ErrorCode.SPANNER_NOT_FOUND;
            }
          }
          case ALREADY_EXISTS -> {
            if (containsIgnoreCase(exception.getMessage(), UNIQUE_INDEX_VIOLATION)) {
              yield ErrorCode.SPANNER_UNIQUE_INDEX_VIOLATION;
            } else {
              yield ErrorCode.SPANNER_ALREADY_EXISTS;
            }
          }
          case FAILED_PRECONDITION -> {
            if (containsIgnoreCase(exception.getMessage(), DATATYPE_MISMATCH)) {
              yield ErrorCode.SPANNER_INVALID_VALUE;
            } else if (containsIgnoreCase(exception.getMessage(), NOT_NULL_VIOLATION)) {
              yield ErrorCode.SPANNER_NULL_VALUE_FOR_REQUIRED_COLUMN;
            } else if (containsIgnoreCase(exception.getMessage(), WRITE_TO_GENERATED_COLUMN)) {
              yield ErrorCode.SPANNER_WRITE_TO_GENERATED_COLUMN_NOT_ALLOWED;
            } else if (containsIgnoreCase(
                    exception.getMessage(), GENERATED_PK_NOT_FULLY_SPECIFIED_1)
                && containsIgnoreCase(exception.getMessage(), GENERATED_PK_NOT_FULLY_SPECIFIED_2)) {
              yield ErrorCode.SPANNER_DEPENDENT_COLUMN_NOT_SPECIFIED;
            } else if (containsIgnoreCase(exception.getMessage(), WRONG_NUMBER_OF_KEY_PARTS)) {
              yield ErrorCode.SPANNER_WRONG_NUMBER_OF_KEY_PARTS;
            } else if (containsIgnoreCase(exception.getMessage(), FK_VIOLATION_CHILD_INSERT_1)
                && containsIgnoreCase(exception.getMessage(), FK_VIOLATION_CHILD_INSERT_2)) {
              yield ErrorCode.SPANNER_FOREIGN_KEY_CONSTRAINT_VIOLATION;
            } else if (containsIgnoreCase(exception.getMessage(), FK_VIOLATION_PARENT_DELETE)) {
              yield ErrorCode.SPANNER_FOREIGN_KEY_CONSTRAINT_VIOLATION;
            } else if (containsIgnoreCase(exception.getMessage(), FK_VIOLATION_PARENT_UPDATE)) {
              yield ErrorCode.SPANNER_FOREIGN_KEY_CONSTRAINT_VIOLATION;
            } else {
              yield ErrorCode.SPANNER_UNKNOWN_ERROR;
            }
          }
          case UNKNOWN -> {
            if (containsIgnoreCase(exception.getMessage(), DUPLICATE_COLUMN)) {
              yield ErrorCode.SPANNER_DUPLICATE_COLUMN;
            } else {
              yield ErrorCode.SPANNER_UNKNOWN_ERROR;
            }
          }
          case DEADLINE_EXCEEDED -> ErrorCode.SPANNER_DEADLINE_EXCEEDED;
          case RESOURCE_EXHAUSTED -> ErrorCode.SPANNER_RESOURCE_EXHAUSTED;
          case ABORTED -> ErrorCode.SPANNER_TRANSACTION_ABORTED;
          default -> ErrorCode.SPANNER_UNKNOWN_ERROR;
        };
    return new SpannerMigrationException(errorCode, exception.getMessage(), exception);
  }

  private static boolean containsIgnoreCase(String s, String subString) {
    if (s == null || subString == null) {
      return false;
    }
    return s.toLowerCase().contains(subString.toLowerCase());
  }
}
