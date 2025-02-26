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
package com.google.cloud.teleport.v2.spanner.exceptions;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.SpannerExceptionParser;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.SpannerMigrationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.Assert;
import org.junit.Test;

/* Unit test for SpannerExceptionParser */
public class SpannerExceptionParserTest {

  private static Map<SpannerException, SpannerMigrationException.ErrorCode> exceptionToExpectedTag =
      new HashMap<>() {
        {
          // Map of SpannerException: ErrorCode

          // ABORTED
          put(
              SpannerExceptionFactory.newSpannerException(ErrorCode.ABORTED, "Transaction Aborted"),
              SpannerMigrationException.ErrorCode.SPANNER_TRANSACTION_ABORTED);
          // ALREADY_EXISTS
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.ALREADY_EXISTS,
                  "Unique index violation on index idx_authors_name at index key [Jane Austen,2]. It conflicts with row [2] in table Authors."),
              SpannerMigrationException.ErrorCode.SPANNER_UNIQUE_INDEX_VIOLATION);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.ALREADY_EXISTS, "Already exists"),
              SpannerMigrationException.ErrorCode.SPANNER_ALREADY_EXISTS);
          // NOT_FOUND
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.NOT_FOUND,
                  "Table not found: AAAA\n"
                      + "resource_type: \"spanner.googleapis.com/Table\"\n"
                      + "resource_name: \"AAAA\"\n"
                      + "description: \"Table not found\n"),
              SpannerMigrationException.ErrorCode.SPANNER_TABLE_NOT_FOUND);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.NOT_FOUND,
                  "Column not found in table Authors: age\n"
                      + "resource_type: \"spanner.googleapis.com/Column\"\n"
                      + "resource_name: \"age\"\n"),
              SpannerMigrationException.ErrorCode.SPANNER_COLUMN_NOT_FOUND);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.NOT_FOUND,
                  "Parent row for row [100,4] in table Books is missing. Row cannot be written."),
              SpannerMigrationException.ErrorCode.SPANNER_PARENT_NOT_FOUND);
          put(
              SpannerExceptionFactory.newSpannerException(ErrorCode.NOT_FOUND, "Not found"),
              SpannerMigrationException.ErrorCode.SPANNER_NOT_FOUND);
          // CANCELLED
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.CANCELLED, "Operation cancelled"),
              SpannerMigrationException.ErrorCode.SPANNER_UNKNOWN_ERROR);
          // DEADLINE_EXCEEDED
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.DEADLINE_EXCEEDED, "Deadline exceeded while processing the request"),
              SpannerMigrationException.ErrorCode.SPANNER_DEADLINE_EXCEEDED);
          // FAILED_PRECONDITION
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION,
                  "Invalid value for column id in table Books: Expected INT64"),
              SpannerMigrationException.ErrorCode.SPANNER_INVALID_VALUE);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION, "title must not be NULL in table Books"),
              SpannerMigrationException.ErrorCode.SPANNER_NULL_VALUE_FOR_REQUIRED_COLUMN);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION,
                  "Cannot write into generated column `Books.titleUpperStored`"),
              SpannerMigrationException.ErrorCode.SPANNER_WRITE_TO_GENERATED_COLUMN_NOT_ALLOWED);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION,
                  "For an Update, the value of a generated primary key `id2` must be explicitly specified, or else its non-key dependent column `part1` must be specified. Key: [6,<default>]"),
              SpannerMigrationException.ErrorCode.SPANNER_DEPENDENT_COLUMN_NOT_SPECIFIED);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION,
                  "Wrong number of key parts for genPK. Expected: 2. Got: [\"6\",\"7\",\"8\"]"),
              SpannerMigrationException.ErrorCode.SPANNER_WRONG_NUMBER_OF_KEY_PARTS);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION,
                  "Foreign key constraint `FK_C1_Books_C0E4C66AABC06BFE_1` is violated on table `C1`. Cannot find referenced values in Books(id)."),
              SpannerMigrationException.ErrorCode.SPANNER_FOREIGN_KEY_CONSTRAINT_VIOLATION);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION,
                  "Integrity constraint violation during DELETE/REPLACE. Found child row [1,1] in table Books"),
              SpannerMigrationException.ErrorCode.SPANNER_FOREIGN_KEY_CONSTRAINT_VIOLATION);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION,
                  "Foreign key constraint violation when deleting or updating referenced row(s): referencing row(s) found in table `FKC`"),
              SpannerMigrationException.ErrorCode.SPANNER_FOREIGN_KEY_CONSTRAINT_VIOLATION);
          // INTERNAL
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.INTERNAL, "Internal Error occurred"),
              SpannerMigrationException.ErrorCode.SPANNER_UNKNOWN_ERROR);
          // INVALID_ARGUMENT
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.INVALID_ARGUMENT,
                  "Invalid commit request: maximum size for a single column value is: 10485760 bytes."),
              SpannerMigrationException.ErrorCode.SPANNER_VALUE_TOO_LARGE);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.INVALID_ARGUMENT, "Invalid argument passed."),
              SpannerMigrationException.ErrorCode.SPANNER_INVALID_ARGUMENT);
          // OUT_OF_RANGE
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.OUT_OF_RANGE,
                  "Check constraint `Authors`.`check_author_id` is violated for key `(-1)`"),
              SpannerMigrationException.ErrorCode.SPANNER_CHECK_CONSTRAINT_VIOLATION);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.OUT_OF_RANGE, "Input is out of range"),
              SpannerMigrationException.ErrorCode.SPANNER_OUT_OF_RANGE);
          // PERMISSION_DENIED
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.PERMISSION_DENIED, "Permission Denied"),
              SpannerMigrationException.ErrorCode.SPANNER_PERMISSION_DENIED);
          // RESOURCE_EXHAUSTED
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.RESOURCE_EXHAUSTED, "Spanner Resource Exhausted"),
              SpannerMigrationException.ErrorCode.SPANNER_RESOURCE_EXHAUSTED);
          // UNAUTHENTICATED
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.UNAUTHENTICATED, "Unauthenticated"),
              SpannerMigrationException.ErrorCode.SPANNER_UNAUTHENTICATED);
          // UNAVAILABLE
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.UNAVAILABLE, "Spanner temporarily Unavailable"),
              SpannerMigrationException.ErrorCode.SPANNER_UNAVAILABLE);
          // UNIMPLEMENTED
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.UNIMPLEMENTED, "Feature used is not implemented"),
              SpannerMigrationException.ErrorCode.SPANNER_UNIMPLEMENTED);
          // UNKNOWN
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.UNKNOWN, "Duplicate column: col2"),
              SpannerMigrationException.ErrorCode.SPANNER_DUPLICATE_COLUMN);
          put(
              SpannerExceptionFactory.newSpannerException(ErrorCode.UNKNOWN, "Unknown exception"),
              SpannerMigrationException.ErrorCode.SPANNER_UNKNOWN_ERROR);
        }
      };

  @Test
  public void testParser() {
    for (Entry<SpannerException, SpannerMigrationException.ErrorCode> entry :
        exceptionToExpectedTag.entrySet()) {
      SpannerMigrationException actual = SpannerExceptionParser.parse(entry.getKey());
      assertSpannerExceptionClassification(entry.getKey(), actual, entry.getValue());
    }
  }

  public static void assertSpannerExceptionClassification(
      SpannerException e,
      SpannerMigrationException actual,
      SpannerMigrationException.ErrorCode expected) {
    Assert.assertEquals(
        String.format(
            "Incorrect SpannerException error code parsing: SpannerException=%s, Actual errorCode=%s, Expected errorCode=%s",
            e.getMessage(), actual.getErrorCode(), expected),
        expected,
        actual.getErrorCode());
    Assert.assertEquals(expected.getCode() + " : " + e.getMessage(), actual.getMessage());
  }
}
