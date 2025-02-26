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

import com.google.cloud.teleport.v2.spanner.migrations.exceptions.SpannerMigrationException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.SpannerMigrationException.ErrorCode;
import java.util.Set;

/**
 * Helper class to classify SpannerExceptions to Retryable error or Permanent error. The exception
 * classification is specific to the DatastreamToSpanner migration template. For example, a "parent
 * not found" error when processing an insert event on a child row is a retryable error for the
 * DatastreamToSpanner template, whereas it is a permanent error for the SourceDbToSpanner template.
 *
 * <p>This classifier is designed to classify Spanner exceptions encountered when the Datastream to
 * Spanner template is run.
 */
public class DatastreamToSpannerExceptionClassifier {

  public enum ErrorTag {
    PERMANENT_ERROR,
    RETRYABLE_ERROR
  }

  /*
  Spanner error codes for Permanent error
   */
  private static Set<ErrorCode> permanentErrorCodes =
      Set.of(
          ErrorCode.SPANNER_UNIQUE_INDEX_VIOLATION,
          ErrorCode.SPANNER_TABLE_NOT_FOUND,
          ErrorCode.SPANNER_COLUMN_NOT_FOUND,
          ErrorCode.SPANNER_INVALID_VALUE,
          ErrorCode.SPANNER_NULL_VALUE_FOR_REQUIRED_COLUMN,
          ErrorCode.SPANNER_WRITE_TO_GENERATED_COLUMN_NOT_ALLOWED,
          ErrorCode.SPANNER_DEPENDENT_COLUMN_NOT_SPECIFIED,
          ErrorCode.SPANNER_WRONG_NUMBER_OF_KEY_PARTS,
          ErrorCode.SPANNER_VALUE_TOO_LARGE,
          ErrorCode.SPANNER_INVALID_ARGUMENT,
          ErrorCode.SPANNER_CHECK_CONSTRAINT_VIOLATION,
          ErrorCode.SPANNER_OUT_OF_RANGE,
          ErrorCode.SPANNER_PERMISSION_DENIED,
          ErrorCode.SPANNER_UNAUTHENTICATED,
          ErrorCode.SPANNER_UNIMPLEMENTED,
          ErrorCode.SPANNER_DUPLICATE_COLUMN);

  public static ErrorTag classify(SpannerMigrationException exception) {
    if (permanentErrorCodes.contains(exception.getErrorCode())) {
      return ErrorTag.PERMANENT_ERROR;
    }
    return ErrorTag.RETRYABLE_ERROR;
  }
}
