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

import com.google.cloud.spanner.SpannerException;
import com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.values.TupleTag;

/* Helper class to classify SpannerExceptions to Retryable error or Permanent error */
public class SpannerExceptionClassifier {

  public enum ErrorTag {
    PERMANENT_ERROR,
    RETRYABLE_ERROR
  }

  /*
  Error message substrings to be inspected from Spanner exception
   */
  // NOT_FOUND
  public static final String TABLE_NOT_FOUND = "Table not found";
  public static final String COLUMN_NOT_FOUND = "Column not found in table";

  // ALREADY_EXISTS
  public static final String UNIQUE_INDEX_VIOLATION = "Unique index violation on index";

  // FAILED_PRECONDITION
  public static final String DATATYPE_MISMATCH = "Invalid value for column";
  public static final String NOT_NULL_VIOLATION = "must not be NULL";
  public static final String WRITE_TO_GENERATED_COLUMN = "Cannot write into generated column";
  public static final String GENERATED_PK_NOT_FULLY_SPECIFIED_1 = "For an Update, the value of a generated primary key";
  public static final String GENERATED_PK_NOT_FULLY_SPECIFIED_2 = "must be explicitly specified, or else its non-key dependent column";
  public static final String DUPLICATE_COLUMN = "Duplicate column";
  public static final String WRONG_NUMBER_OF_KEY_PARTS = "Wrong number of key parts";

  public static ErrorTag classify(SpannerException exception) {
    switch (exception.getErrorCode()) {
      case UNIMPLEMENTED, UNAUTHENTICATED, PERMISSION_DENIED, OUT_OF_RANGE, INVALID_ARGUMENT:
        return ErrorTag.PERMANENT_ERROR;
      case NOT_FOUND:
        if (containsIgnoreCase(exception.getMessage(), TABLE_NOT_FOUND)) {
          return ErrorTag.PERMANENT_ERROR;
        }
        if (containsIgnoreCase(exception.getMessage(), COLUMN_NOT_FOUND)) {
          return ErrorTag.PERMANENT_ERROR;
        }
        break;
      case ALREADY_EXISTS:
        if (containsIgnoreCase(exception.getMessage(), UNIQUE_INDEX_VIOLATION)) {
          return ErrorTag.PERMANENT_ERROR;
        }
        break;
      case FAILED_PRECONDITION:
        if (containsIgnoreCase(exception.getMessage(), DATATYPE_MISMATCH)) {
          return ErrorTag.PERMANENT_ERROR;
        }
        if (containsIgnoreCase(exception.getMessage(), NOT_NULL_VIOLATION)) {
          return ErrorTag.PERMANENT_ERROR;
        }
        if (containsIgnoreCase(exception.getMessage(), WRITE_TO_GENERATED_COLUMN)) {
          return ErrorTag.PERMANENT_ERROR;
        }
        if (containsIgnoreCase(exception.getMessage(), GENERATED_PK_NOT_FULLY_SPECIFIED_1) && containsIgnoreCase(exception.getMessage(), GENERATED_PK_NOT_FULLY_SPECIFIED_2)) {
          return ErrorTag.PERMANENT_ERROR;
        }
        if (containsIgnoreCase(exception.getMessage(), WRONG_NUMBER_OF_KEY_PARTS)) {
          return ErrorTag.PERMANENT_ERROR;
        }
        break;
      case UNKNOWN:
        if (containsIgnoreCase(exception.getMessage(), DUPLICATE_COLUMN)) {
          return ErrorTag.PERMANENT_ERROR;
        }
        break;
    }
    return ErrorTag.RETRYABLE_ERROR;
  }

  private static boolean containsIgnoreCase(String s, String subString) {
    return s.toLowerCase().contains(subString.toLowerCase());
  }
}
