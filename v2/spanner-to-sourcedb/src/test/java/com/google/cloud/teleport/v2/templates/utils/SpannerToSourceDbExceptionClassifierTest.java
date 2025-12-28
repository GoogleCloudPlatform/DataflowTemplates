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

import static org.junit.Assert.assertEquals;

import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import java.sql.SQLDataException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for SpannerToSourceDbExceptionClassifier. */
@RunWith(JUnit4.class)
public class SpannerToSourceDbExceptionClassifierTest {

  @Test
  public void testClassifySpannerExceptionPermanentCodes() {
    ErrorCode[] permanentCodes = {
      ErrorCode.ALREADY_EXISTS,
      ErrorCode.OUT_OF_RANGE,
      ErrorCode.INVALID_ARGUMENT,
      ErrorCode.NOT_FOUND,
      ErrorCode.FAILED_PRECONDITION,
      ErrorCode.PERMISSION_DENIED,
      ErrorCode.UNAUTHENTICATED,
      ErrorCode.UNIMPLEMENTED,
      ErrorCode.INTERNAL,
    };

    for (ErrorCode code : permanentCodes) {
      SpannerException ex = SpannerExceptionFactory.newSpannerException(code, "test");
      TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex);
      assertEquals("Expected PERMANENT for " + code, Constants.PERMANENT_ERROR_TAG, tag);
    }
  }

  @Test
  public void testClassifySpannerExceptionRetryableCodes() {
    ErrorCode[] retryableCodes = {
      ErrorCode.UNAVAILABLE,
      ErrorCode.ABORTED,
      ErrorCode.DEADLINE_EXCEEDED,
      ErrorCode.UNKNOWN,
      ErrorCode.RESOURCE_EXHAUSTED,
      ErrorCode.CANCELLED,
    };

    for (ErrorCode code : retryableCodes) {
      SpannerException ex = SpannerExceptionFactory.newSpannerException(code, "test");
      TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex);
      assertEquals("Expected RETRYABLE for " + code, Constants.RETRYABLE_ERROR_TAG, tag);
    }
  }

  @Test
  public void testClassifyChangeEventConvertorException() {
    Exception ex = new ChangeEventConvertorException("test");
    TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex);
    assertEquals(Constants.PERMANENT_ERROR_TAG, tag);
  }

  @Test
  public void testClassifyWrappedInvalidTransformationException() {
    SpannerException ex =
        SpannerExceptionFactory.newSpannerException(
            ErrorCode.UNKNOWN, "test", new InvalidTransformationException("test"));
    TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex);
    assertEquals(Constants.PERMANENT_ERROR_TAG, tag);
  }

  @Test
  public void testClassifyWrappedChangeEventConvertorException() {
    SpannerException ex =
        SpannerExceptionFactory.newSpannerException(
            ErrorCode.UNKNOWN, "test", new ChangeEventConvertorException("test"));
    TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex);
    assertEquals(Constants.PERMANENT_ERROR_TAG, tag);
  }

  @Test
  public void testClassifyWrappedCodecNotFoundException() {
    CodecNotFoundException mockException = org.mockito.Mockito.mock(CodecNotFoundException.class);
    SpannerException ex =
        SpannerExceptionFactory.newSpannerException(ErrorCode.UNKNOWN, "test", mockException);
    TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex);
    assertEquals(Constants.PERMANENT_ERROR_TAG, tag);
  }

  @Test
  public void testClassifyWrappedSQLSyntaxErrorException() {
    SpannerException ex =
        SpannerExceptionFactory.newSpannerException(
            ErrorCode.UNKNOWN, "test", new SQLSyntaxErrorException("test"));
    TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex);
    assertEquals(Constants.PERMANENT_ERROR_TAG, tag);
  }

  @Test
  public void testClassifyWrappedSQLDataException() {
    SpannerException ex =
        SpannerExceptionFactory.newSpannerException(
            ErrorCode.UNKNOWN, "test", new SQLDataException("test"));
    TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex);
    assertEquals(Constants.PERMANENT_ERROR_TAG, tag);
  }

  @Test
  public void testClassifyWrappedSQLNonTransientConnectionExceptionPermanent() {
    SpannerException ex =
        SpannerExceptionFactory.newSpannerException(
            ErrorCode.UNKNOWN,
            "test",
            new SQLNonTransientConnectionException("test", "state", 9999));
    TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex);
    assertEquals(Constants.PERMANENT_ERROR_TAG, tag);
  }

  @Test
  public void testClassifyWrappedSQLNonTransientConnectionExceptionRetryable() {
    int[] retryableSqlCodes = {1053, 1159, 1161};
    for (int code : retryableSqlCodes) {
      SpannerException ex =
          SpannerExceptionFactory.newSpannerException(
              ErrorCode.UNKNOWN,
              "test",
              new SQLNonTransientConnectionException("test", "state", code));
      TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex);
      assertEquals("Expected RETRYABLE for SQL code " + code, Constants.RETRYABLE_ERROR_TAG, tag);
    }
  }

  @Test
  public void testClassifyIllegalArgumentException() {
    Exception ex = new IllegalArgumentException("test");
    TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex);
    assertEquals(Constants.PERMANENT_ERROR_TAG, tag);
  }

  @Test
  public void testClassifyNullPointerException() {
    Exception ex = new NullPointerException("test");
    TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex);
    assertEquals(Constants.PERMANENT_ERROR_TAG, tag);
  }

  @Test
  public void testClassifyGenericException() {
    Exception ex = new RuntimeException("test");
    TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex);
    assertEquals(Constants.RETRYABLE_ERROR_TAG, tag);
  }
}
