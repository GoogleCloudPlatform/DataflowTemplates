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

public class SpannerMigrationException extends Exception {
  public enum ErrorCode {
    // Errors thrown from Spanner
    SPANNER_TRANSACTION_ABORTED(1001),
    SPANNER_UNIQUE_INDEX_VIOLATION(1002),
    SPANNER_TABLE_NOT_FOUND(1003),
    SPANNER_COLUMN_NOT_FOUND(1004),
    SPANNER_PARENT_NOT_FOUND(1005),
    SPANNER_DEADLINE_EXCEEDED(1006),
    SPANNER_INVALID_VALUE(1007),
    SPANNER_NULL_VALUE_FOR_REQUIRED_COLUMN(1008),
    SPANNER_WRITE_TO_GENERATED_COLUMN_NOT_ALLOWED(1009),
    SPANNER_DEPENDENT_COLUMN_NOT_SPECIFIED(1010),
    SPANNER_WRONG_NUMBER_OF_KEY_PARTS(1011),
    SPANNER_FOREIGN_KEY_CONSTRAINT_VIOLATION(1012),
    SPANNER_VALUE_TOO_LARGE(1013),
    SPANNER_INVALID_ARGUMENT(1014),
    SPANNER_CHECK_CONSTRAINT_VIOLATION(1015),
    SPANNER_OUT_OF_RANGE(1016),
    SPANNER_PERMISSION_DENIED(1017),
    SPANNER_RESOURCE_EXHAUSTED(1018),
    SPANNER_UNAUTHENTICATED(1019),
    SPANNER_UNIMPLEMENTED(1020),
    SPANNER_DUPLICATE_COLUMN(1021),
    SPANNER_UNAVAILABLE(1022),
    SPANNER_ALREADY_EXISTS(1023),
    SPANNER_NOT_FOUND(1024),
    SPANNER_UNKNOWN_ERROR(1200), // SpannerException catch all error code

    // Conversion and Transformation errors
    AVRO_TYPE_CONVERSION_ERROR(2001),
    CHANGE_EVENT_CONVERSION_ERROR(2002),
    INVALID_CHANGE_EVENT(2003),
    CUSTOM_TRANSFORMATION_ERROR(2004),

    // Errors from Source Database

    // Generic error
    UNKNOWN_ERROR(9999);

    private final int code;

    ErrorCode(int code) {
      this.code = code;
    }

    public int getCode() {
      return code;
    }
  }

  private final ErrorCode errorCode;

  public SpannerMigrationException(ErrorCode errorCode, String message) {
    super(errorCode.getCode() + " - " + message);
    this.errorCode = errorCode;
  }

  public SpannerMigrationException(ErrorCode errorCode, String message, Throwable cause) {
    super(errorCode.getCode() + " - " + message, cause);
    this.errorCode = errorCode;
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }
}
