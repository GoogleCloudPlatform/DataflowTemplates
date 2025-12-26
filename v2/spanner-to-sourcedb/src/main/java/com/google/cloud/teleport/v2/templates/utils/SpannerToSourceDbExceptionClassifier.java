/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates.utils;

import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import java.sql.SQLDataException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.util.Set;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Helper class to classify SpannerExceptions to Retryable error or Permanent
 * error.
 */
public class SpannerToSourceDbExceptionClassifier {

  private static final Set<ErrorCode> permanentErrorCodes = Set.of(
      ErrorCode.ALREADY_EXISTS,
      ErrorCode.OUT_OF_RANGE,
      ErrorCode.INVALID_ARGUMENT,
      ErrorCode.NOT_FOUND,
      ErrorCode.FAILED_PRECONDITION,
      ErrorCode.PERMISSION_DENIED,
      ErrorCode.UNAUTHENTICATED,
      ErrorCode.RESOURCE_EXHAUSTED,
      ErrorCode.UNIMPLEMENTED);

  public static TupleTag<String> classify(Exception exception) {
    if (exception instanceof SpannerException e) {
      return classifySpannerException(e);
    } else if (exception instanceof ChangeEventConvertorException
        || exception instanceof IllegalArgumentException
        || exception instanceof NullPointerException) {
      return Constants.PERMANENT_ERROR_TAG;
    }
    return Constants.RETRYABLE_ERROR_TAG;
  }

  private static TupleTag<String> classifySpannerException(SpannerException exception) {
    // Since we have wrapped the logic inside Spanner transaction, the exceptions
    // would also be
    // wrapped inside a SpannerException.
    // We need to get and inspect the cause while handling the exception.
    Throwable cause = exception.getCause();

    if (cause instanceof InvalidTransformationException
        || cause instanceof ChangeEventConvertorException) {
      return Constants.PERMANENT_ERROR_TAG;
    } else if (cause instanceof CodecNotFoundException
        || cause instanceof SQLSyntaxErrorException
        || cause instanceof SQLDataException) {
      return Constants.PERMANENT_ERROR_TAG;
    } else if (cause instanceof SQLNonTransientConnectionException e
        && e.getErrorCode() != 1053
        && e.getErrorCode() != 1159
        && e.getErrorCode() != 1161) {
      // https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
      // error codes 1053,1161 and 1159 can be retried
      return Constants.PERMANENT_ERROR_TAG;
    }

    if (permanentErrorCodes.contains(exception.getErrorCode())) {
      return Constants.PERMANENT_ERROR_TAG;
    }
    return Constants.RETRYABLE_ERROR_TAG;
  }
}
