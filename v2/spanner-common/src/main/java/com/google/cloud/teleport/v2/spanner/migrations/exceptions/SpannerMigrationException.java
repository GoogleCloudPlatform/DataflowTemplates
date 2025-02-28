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

public class SpannerMigrationException extends RuntimeException {
  public enum ErrorCode {
    // Errors thrown from Spanner
    SPANNER_TRANSACTION_ABORTED("SMT-1001"),
    SPANNER_UNIQUE_INDEX_VIOLATION("SMT-1002"),
    SPANNER_TABLE_NOT_FOUND("SMT-1003"),
    SPANNER_COLUMN_NOT_FOUND("SMT-1004"),
    SPANNER_PARENT_NOT_FOUND("SMT-1005"),
    SPANNER_DEADLINE_EXCEEDED("SMT-1006"),
    SPANNER_INVALID_VALUE("SMT-1007"),
    SPANNER_NULL_VALUE_FOR_REQUIRED_COLUMN("SMT-1008"),
    SPANNER_WRITE_TO_GENERATED_COLUMN_NOT_ALLOWED("SMT-1009"),
    SPANNER_DEPENDENT_COLUMN_NOT_SPECIFIED("SMT-1010"),
    SPANNER_WRONG_NUMBER_OF_KEY_PARTS("SMT-1011"),
    SPANNER_FOREIGN_KEY_CONSTRAINT_VIOLATION("SMT-1012"),
    SPANNER_VALUE_TOO_LARGE("SMT-1013"),
    SPANNER_INVALID_ARGUMENT("SMT-1014"),
    SPANNER_CHECK_CONSTRAINT_VIOLATION("SMT-1015"),
    SPANNER_OUT_OF_RANGE("SMT-1016"),
    SPANNER_PERMISSION_DENIED("SMT-1017"),
    SPANNER_RESOURCE_EXHAUSTED("SMT-1018"),
    SPANNER_UNAUTHENTICATED("SMT-1019"),
    SPANNER_UNIMPLEMENTED("SMT-1020"),
    SPANNER_DUPLICATE_COLUMN("SMT-1021"),
    SPANNER_UNAVAILABLE("SMT-1022"),
    SPANNER_ALREADY_EXISTS("SMT-1023"),
    SPANNER_NOT_FOUND("SMT-1024"),
    SPANNER_UNKNOWN_ERROR("SMT-1200"), // SpannerException catch all error code

    // Conversion and Transformation errors
    AVRO_TYPE_CONVERSION_ERROR("SMT-2001"),
    CHANGE_EVENT_CONVERSION_ERROR("SMT-2002"),
    INVALID_CHANGE_EVENT("SMT-2003"),
    CUSTOM_TRANSFORMATION_ERROR("SMT-2004"),

    // Errors from Source Database

    // Generic error
    UNKNOWN_ERROR("SMT-9999");

    private final String code;

    ErrorCode(String code) {
      this.code = code;
    }

    public String getCode() {
      return code;
    }
  }

  private final ErrorCode errorCode;

  public SpannerMigrationException(ErrorCode errorCode, String message) {
    super(errorCode.getCode() + " : " + message);
    this.errorCode = errorCode;
  }

  public SpannerMigrationException(ErrorCode errorCode, String message, Throwable cause) {
    super(errorCode.getCode() + " : " + message, cause);
    this.errorCode = errorCode;
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }
}
