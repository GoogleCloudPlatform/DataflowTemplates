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
package com.google.cloud.teleport.v2.templates.spanner;

import static com.google.cloud.teleport.v2.templates.spanner.DatastreamToSpannerExceptionClassifier.ErrorTag.PERMANENT_ERROR;
import static com.google.cloud.teleport.v2.templates.spanner.DatastreamToSpannerExceptionClassifier.ErrorTag.RETRYABLE_ERROR;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.SpannerExceptionParser;
import com.google.cloud.teleport.v2.templates.spanner.DatastreamToSpannerExceptionClassifier.ErrorTag;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.Assert;
import org.junit.Test;

/* Unit test for SpannerExceptionClassifier */
public class DatastreamToSpannerExceptionClassifierTest {

  private static Map<SpannerException, ErrorTag> exceptionToExpectedTag =
      new HashMap<>() {
        {
          // Map of SpannerException: Expected tag

          // ABORTED
          put(
              SpannerExceptionFactory.newSpannerException(ErrorCode.ABORTED, "Transaction Aborted"),
              RETRYABLE_ERROR);
          // ALREADY_EXISTS
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.ALREADY_EXISTS,
                  "Unique index violation on index idx_authors_name at index key [Jane Austen,2]. It conflicts with row [2] in table Authors."),
              PERMANENT_ERROR);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.ALREADY_EXISTS, "Already exists"),
              RETRYABLE_ERROR);
          // NOT_FOUND
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.NOT_FOUND,
                  "Table not found: AAAA\n"
                      + "resource_type: \"spanner.googleapis.com/Table\"\n"
                      + "resource_name: \"AAAA\"\n"
                      + "description: \"Table not found\n"),
              PERMANENT_ERROR);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.NOT_FOUND,
                  "Column not found in table Authors: age\n"
                      + "resource_type: \"spanner.googleapis.com/Column\"\n"
                      + "resource_name: \"age\"\n"),
              PERMANENT_ERROR);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.NOT_FOUND,
                  "Parent row for row [100,4] in table Books is missing. Row cannot be written."),
              RETRYABLE_ERROR);
          put(
              SpannerExceptionFactory.newSpannerException(ErrorCode.NOT_FOUND, "Not found"),
              RETRYABLE_ERROR);
          // CANCELLED
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.CANCELLED, "Operation cancelled"),
              RETRYABLE_ERROR);
          // DEADLINE_EXCEEDED
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.DEADLINE_EXCEEDED, "Deadline exceeded while processing the request"),
              RETRYABLE_ERROR);
          // FAILED_PRECONDITION
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION,
                  "Invalid value for column id in table Books: Expected INT64"),
              PERMANENT_ERROR);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION, "title must not be NULL in table Books"),
              PERMANENT_ERROR);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION,
                  "Cannot write into generated column `Books.titleUpperStored`"),
              PERMANENT_ERROR);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION,
                  "For an Update, the value of a generated primary key `id2` must be explicitly specified, or else its non-key dependent column `part1` must be specified. Key: [6,<default>]"),
              PERMANENT_ERROR);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION,
                  "Wrong number of key parts for genPK. Expected: 2. Got: [\"6\",\"7\",\"8\"]"),
              PERMANENT_ERROR);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION,
                  "Foreign key constraint `FK_C1_Books_C0E4C66AABC06BFE_1` is violated on table `C1`. Cannot find referenced values in Books(id)."),
              RETRYABLE_ERROR);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION,
                  "Integrity constraint violation during DELETE/REPLACE. Found child row [1,1] in table Books"),
              RETRYABLE_ERROR);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION,
                  "Foreign key constraint violation when deleting or updating referenced row(s): referencing row(s) found in table `FKC`"),
              RETRYABLE_ERROR);
          // INTERNAL
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.INTERNAL, "Internal Error occurred"),
              RETRYABLE_ERROR);
          // INVALID_ARGUMENT
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.INVALID_ARGUMENT,
                  "Invalid commit request: maximum size for a single column value is: 10485760 bytes."),
              PERMANENT_ERROR);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.INVALID_ARGUMENT, "Invalid argument passed."),
              PERMANENT_ERROR);
          // OUT_OF_RANGE
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.OUT_OF_RANGE,
                  "Check constraint `Authors`.`check_author_id` is violated for key `(-1)`"),
              PERMANENT_ERROR);
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.OUT_OF_RANGE, "Input is out of range"),
              PERMANENT_ERROR);
          // PERMISSION_DENIED
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.PERMISSION_DENIED, "Permission Denied"),
              PERMANENT_ERROR);
          // RESOURCE_EXHAUSTED
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.RESOURCE_EXHAUSTED, "Spanner Resource Exhausted"),
              RETRYABLE_ERROR);
          // UNAUTHENTICATED
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.UNAUTHENTICATED, "Unauthenticated"),
              PERMANENT_ERROR);
          // UNAVAILABLE
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.UNAVAILABLE, "Spanner temporarily Unavailable"),
              RETRYABLE_ERROR);
          // UNIMPLEMENTED
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.UNIMPLEMENTED, "Feature used is not implemented"),
              PERMANENT_ERROR);
          // UNKNOWN
          put(
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.UNKNOWN, "Duplicate column: col2"),
              PERMANENT_ERROR);
          put(
              SpannerExceptionFactory.newSpannerException(ErrorCode.UNKNOWN, "Unknown exception"),
              RETRYABLE_ERROR);
        }
      };

  @Test
  public void testSpannerExceptionClassification() {
    for (Entry<SpannerException, ErrorTag> entry : exceptionToExpectedTag.entrySet()) {
      ErrorTag actual =
          DatastreamToSpannerExceptionClassifier.classify(
              SpannerExceptionParser.parse(entry.getKey()));
      assertSpannerExceptionClassification(entry.getKey(), actual, entry.getValue());
    }
  }

  public static void assertSpannerExceptionClassification(
      SpannerException e, ErrorTag actual, ErrorTag expected) {
    if (expected == RETRYABLE_ERROR) {
      Assert.assertEquals(
          String.format(
              "Incorrect SpannerException classification: SpannerException=%s Classification=RETRYABLE_ERROR_TAG",
              e.getMessage()),
          expected,
          actual);
    } else {
      Assert.assertEquals(
          String.format(
              "Incorrect SpannerException classification: SpannerException=%s Classification=PERMANENT_ERROR_TAG",
              e.getMessage()),
          expected,
          actual);
    }
  }
}
