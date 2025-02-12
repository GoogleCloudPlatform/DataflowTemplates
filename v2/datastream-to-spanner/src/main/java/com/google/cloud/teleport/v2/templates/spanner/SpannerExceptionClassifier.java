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
  public static final String GENERATED_PK_NOT_FULLY_SPECIFIED_1 = "For an Update, the value of a generated primary key"
  public static final String GENERATED_PK_NOT_FULLY_SPECIFIED_2 = "must be explicitly specified, or else its non-key dependent column";
  public static final String DUPLICATE_COLUMN = "Duplicate column";
  public static final String WRONG_NUMBER_OF_KEY_PARTS = "Wrong number of key parts";

  public static TupleTag<FailsafeElement<String, String>> classify(SpannerException exception) {
    switch (exception.getErrorCode()) {
      case UNIMPLEMENTED, UNAUTHENTICATED, PERMISSION_DENIED, OUT_OF_RANGE, INVALID_ARGUMENT:
        return DatastreamToSpannerConstants.PERMANENT_ERROR_TAG;
      case NOT_FOUND:
        if (exception.getMessage().equalsIgnoreCase(TABLE_NOT_FOUND)) {
          return DatastreamToSpannerConstants.PERMANENT_ERROR_TAG;
        }
        if (exception.getMessage().equalsIgnoreCase(COLUMN_NOT_FOUND)) {
          return DatastreamToSpannerConstants.PERMANENT_ERROR_TAG;
        }
        break;
      case ALREADY_EXISTS:
        if (exception.getMessage().equalsIgnoreCase(UNIQUE_INDEX_VIOLATION)) {
          return DatastreamToSpannerConstants.PERMANENT_ERROR_TAG;
        }
        break;
      case FAILED_PRECONDITION:
        if (exception.getMessage().equalsIgnoreCase(DATATYPE_MISMATCH)) {
          return DatastreamToSpannerConstants.PERMANENT_ERROR_TAG;
        }
        if (exception.getMessage().equalsIgnoreCase(NOT_NULL_VIOLATION)) {
          return DatastreamToSpannerConstants.PERMANENT_ERROR_TAG;
        }
        if (exception.getMessage().equalsIgnoreCase(WRITE_TO_GENERATED_COLUMN)) {
          return DatastreamToSpannerConstants.PERMANENT_ERROR_TAG;
        }
        if (exception.getMessage().equalsIgnoreCase(GENERATED_PK_NOT_FULLY_SPECIFIED_1) && exception.getMessage().equalsIgnoreCase(GENERATED_PK_NOT_FULLY_SPECIFIED_2)) {
          return DatastreamToSpannerConstants.PERMANENT_ERROR_TAG;
        }
        if (exception.getMessage().equalsIgnoreCase(DUPLICATE_COLUMN)) {
          return DatastreamToSpannerConstants.PERMANENT_ERROR_TAG;
        }
        if (exception.getMessage().equalsIgnoreCase(WRONG_NUMBER_OF_KEY_PARTS)) {
          return DatastreamToSpannerConstants.PERMANENT_ERROR_TAG;
        }
        break;
    }
    return DatastreamToSpannerConstants.RETRYABLE_ERROR_TAG;
  }
}
