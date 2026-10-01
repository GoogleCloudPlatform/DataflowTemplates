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

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.dbutils.processor.ISpToSrcSourceConnector;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import java.sql.SQLDataException;
import java.sql.SQLSyntaxErrorException;
import java.util.Set;
import org.apache.beam.sdk.values.TupleTag;

/** Helper class to classify Exceptions into Retryable or Permanent error. */
public class SpannerToSourceDbExceptionClassifier {

  private static final Set<ErrorCode> permanentSpannerErrorCodes =
      Set.of(
          ErrorCode.ALREADY_EXISTS,
          ErrorCode.OUT_OF_RANGE,
          ErrorCode.INVALID_ARGUMENT,
          ErrorCode.FAILED_PRECONDITION,
          ErrorCode.PERMISSION_DENIED,
          ErrorCode.UNAUTHENTICATED,
          ErrorCode.UNIMPLEMENTED,
          ErrorCode.INTERNAL);

  public static TupleTag<String> classify(Exception exception, ISpToSrcSourceConnector connector) {
    if (exception instanceof SpannerException e) {
      return classifySpannerException(e, connector);
    } else if (exception instanceof ChangeEventConvertorException
        || exception instanceof IllegalArgumentException
        || exception instanceof InvalidDMLGenerationException
        || exception instanceof NullPointerException) {
      return Constants.PERMANENT_ERROR_TAG;
    }
    return Constants.RETRYABLE_ERROR_TAG;
  }

  private static TupleTag<String> classifySpannerException(
      SpannerException exception, ISpToSrcSourceConnector connector) {
    // child exceptions are wrapped inside SpannerException.
    Throwable cause = exception.getCause();

    if (cause instanceof InvalidTransformationException
        || cause instanceof ChangeEventConvertorException
        || cause instanceof InvalidDMLGenerationException) {
      return Constants.PERMANENT_ERROR_TAG;
    } else if (cause instanceof SQLSyntaxErrorException || cause instanceof SQLDataException) {
      return Constants.PERMANENT_ERROR_TAG;
    }

    if (connector != null) {
      TupleTag<String> dialectTag = connector.classifyException(cause);
      if (dialectTag != null) {
        return dialectTag;
      }
    }

    if (permanentSpannerErrorCodes.contains(exception.getErrorCode())) {
      return Constants.PERMANENT_ERROR_TAG;
    }
    return Constants.RETRYABLE_ERROR_TAG;
  }
}
