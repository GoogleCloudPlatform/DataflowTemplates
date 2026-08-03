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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.dbutils.processor.ISpToSrcSourceConnector;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
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
      TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex, null);
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
      TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex, null);
      assertEquals("Expected RETRYABLE for " + code, Constants.RETRYABLE_ERROR_TAG, tag);
    }
  }

  @Test
  public void testClassifyChangeEventConvertorException() {
    Exception ex = new ChangeEventConvertorException("test");
    TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex, null);
    assertEquals(Constants.PERMANENT_ERROR_TAG, tag);
  }

  @Test
  public void testClassifyWrappedInvalidTransformationException() {
    SpannerException ex =
        SpannerExceptionFactory.newSpannerException(
            ErrorCode.UNKNOWN, "test", new InvalidTransformationException("test"));
    TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex, null);
    assertEquals(Constants.PERMANENT_ERROR_TAG, tag);
  }

  @Test
  public void testClassifyWrappedChangeEventConvertorException() {
    SpannerException ex =
        SpannerExceptionFactory.newSpannerException(
            ErrorCode.UNKNOWN, "test", new ChangeEventConvertorException("test"));
    TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex, null);
    assertEquals(Constants.PERMANENT_ERROR_TAG, tag);
  }

  @Test
  public void testClassifyIllegalArgumentException() {
    Exception ex = new IllegalArgumentException("test");
    TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex, null);
    assertEquals(Constants.PERMANENT_ERROR_TAG, tag);
  }

  @Test
  public void testClassifyNullPointerException() {
    Exception ex = new NullPointerException("test");
    TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex, null);
    assertEquals(Constants.PERMANENT_ERROR_TAG, tag);
  }

  @Test
  public void testClassifyGenericException() {
    Exception ex = new RuntimeException("test");
    TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(ex, null);
    assertEquals(Constants.RETRYABLE_ERROR_TAG, tag);
  }

  @Test
  public void testClassifyInvalidDMLGenerationException() {
    InvalidDMLGenerationException exception =
        new InvalidDMLGenerationException("Unknown column present in source table");
    TupleTag<String> tag = SpannerToSourceDbExceptionClassifier.classify(exception, null);
    assertEquals(Constants.PERMANENT_ERROR_TAG, tag);
  }

  @Test
  public void testClassifyDelegatesToConnector() {
    Throwable dialectEx = new RuntimeException("dialect exception");
    SpannerException spannerEx = SpannerExceptionFactory.newSpannerException(dialectEx);
    ISpToSrcSourceConnector mockConnector = mock(ISpToSrcSourceConnector.class);

    // 1. Connector returns permanent
    when(mockConnector.classifyException(dialectEx)).thenReturn(Constants.PERMANENT_ERROR_TAG);
    TupleTag<String> tag1 = SpannerToSourceDbExceptionClassifier.classify(spannerEx, mockConnector);
    assertEquals(Constants.PERMANENT_ERROR_TAG, tag1);

    // 2. Connector returns retryable
    when(mockConnector.classifyException(dialectEx)).thenReturn(Constants.RETRYABLE_ERROR_TAG);
    TupleTag<String> tag2 = SpannerToSourceDbExceptionClassifier.classify(spannerEx, mockConnector);
    assertEquals(Constants.RETRYABLE_ERROR_TAG, tag2);

    // 3. Connector returns null (fallback to general, which should be retryable for generic
    // RuntimeException, and generic SpannerException with UNKNOWN error code is retryable)
    when(mockConnector.classifyException(dialectEx)).thenReturn(null);
    TupleTag<String> tag3 = SpannerToSourceDbExceptionClassifier.classify(spannerEx, mockConnector);
    assertEquals(Constants.RETRYABLE_ERROR_TAG, tag3);
  }
}
