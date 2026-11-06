/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.spanner.testutils.retry;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.MultipleFailureException;

/** Tests for {@link TransientFailureClassifier}. */
@RunWith(JUnit4.class)
public class TransientFailureClassifierTest {

  @After
  public void clearExtraRegex() {
    System.clearProperty(TransientFailureClassifier.EXTRA_RETRYABLE_REGEX_PROPERTY);
  }

  @Test
  public void genuineFailureTypes_areGenuine() {
    assertThat(TransientFailureClassifier.isGenuine(new AssertionError("expected 5 rows")))
        .isTrue();
    assertThat(TransientFailureClassifier.isGenuine(new NullPointerException("null column")))
        .isTrue();
    assertThat(TransientFailureClassifier.isGenuine(new IllegalArgumentException("bad arg")))
        .isTrue();
    assertThat(TransientFailureClassifier.isGenuine(new IllegalStateException("bad state")))
        .isTrue();
    assertThat(TransientFailureClassifier.isGenuine(new ClassCastException("bad cast"))).isTrue();
    assertThat(TransientFailureClassifier.isGenuine(new NoClassDefFoundError("missing class")))
        .isTrue();
  }

  @Test
  public void otherFailures_areTransient() {
    assertThat(TransientFailureClassifier.isGenuine(new RuntimeException("job failed"))).isFalse();
    assertThat(TransientFailureClassifier.isGenuine(new IOException("connection reset"))).isFalse();
    assertThat(TransientFailureClassifier.isGenuine(new TimeoutException("timed out"))).isFalse();
  }

  @Test
  public void genuineTypeInCauseChain_isGenuine() {
    Throwable failure =
        new RuntimeException("wrapper", new IllegalStateException("unexpected schema"));

    assertThat(TransientFailureClassifier.isGenuine(failure)).isTrue();
  }

  @Test
  public void retryableMessage_isTransientEvenForGenuineType() {
    Throwable failure =
        new IllegalStateException(
            "Container is started, but cannot be accessed by (JDBC URL:"
                + " jdbc:mysql://localhost:1234/test), please check container logs");

    assertThat(TransientFailureClassifier.isGenuine(failure)).isFalse();
    assertThat(TransientFailureClassifier.matchingRetryablePattern(failure).pattern())
        .isEqualTo("Container is started, but cannot be accessed");
  }

  @Test
  public void retryableMessageInCauseChain_isTransient() {
    Throwable failure =
        new IllegalStateException(
            "setup failed", new RuntimeException("Communications link failure to MySQL"));

    assertThat(TransientFailureClassifier.isGenuine(failure)).isFalse();
    assertThat(TransientFailureClassifier.matchingRetryablePattern(failure).pattern())
        .isEqualTo("Communications link failure");
  }

  @Test
  public void retryablePatterns_matchKnownInfraErrors() {
    assertThat(
            TransientFailureClassifier.matchingRetryablePattern(
                new RuntimeException(
                    "Operation with name \"projects/p/locations/us-central1/operations/op-1\""
                        + " failed with status = GrpcStatusCode{transportCode=INTERNAL}")))
        .isNotNull();
    assertThat(
            TransientFailureClassifier.matchingRetryablePattern(
                new RuntimeException(
                    "Operation with name \"op-2\" failed with status ="
                        + " GrpcStatusCode{transportCode=UNKNOWN} and message = Unknown")))
        .isNotNull();
    assertThat(
            TransientFailureClassifier.matchingRetryablePattern(
                new RuntimeException("Query timed out after PT2S")))
        .isNotNull();
    assertThat(
            TransientFailureClassifier.matchingRetryablePattern(
                new RuntimeException("503 Service Unavailable")))
        .isNotNull();
    assertThat(
            TransientFailureClassifier.matchingRetryablePattern(
                new RuntimeException(" (code=502)<html>")))
        .isNotNull();
    assertThat(
            TransientFailureClassifier.matchingRetryablePattern(
                new RuntimeException("Datastream: Unable to queue the operation")))
        .isNotNull();
    assertThat(
            TransientFailureClassifier.matchingRetryablePattern(
                new RuntimeException("The connection attempt failed.")))
        .isNotNull();
  }

  @Test
  public void dataflowStockoutMessages_areNotRetryablePatterns() {
    // A failed Dataflow job only surfaces a generic "Job ... failed" message to the test.
    assertThat(
            TransientFailureClassifier.matchingRetryablePattern(
                new AssertionError("ZONE_RESOURCE_POOL_EXHAUSTED")))
        .isNull();
    assertThat(
            TransientFailureClassifier.matchingRetryablePattern(
                new AssertionError("zone does not have enough resources available")))
        .isNull();
  }

  @Test
  public void noRetryablePattern_returnsNull() {
    assertThat(TransientFailureClassifier.matchingRetryablePattern(new AssertionError("wrong")))
        .isNull();
    assertThat(TransientFailureClassifier.matchingRetryablePattern(new RuntimeException()))
        .isNull();
  }

  @Test
  public void extraRetryableRegex_fromSystemProperty() {
    Throwable failure = new AssertionError("Spanner returned RESOURCE_EXHAUSTED: try again");
    assertThat(TransientFailureClassifier.isGenuine(failure)).isTrue();

    System.setProperty(
        TransientFailureClassifier.EXTRA_RETRYABLE_REGEX_PROPERTY, "RESOURCE_EXHAUSTED");

    assertThat(TransientFailureClassifier.isGenuine(failure)).isFalse();
    assertThat(TransientFailureClassifier.matchingRetryablePattern(failure).pattern())
        .isEqualTo("RESOURCE_EXHAUSTED");
  }

  @Test
  public void multipleFailures_genuineIfAnyIsGenuine() {
    Throwable genuineAndTransient =
        new MultipleFailureException(
            List.of(new RuntimeException("cleanup failed"), new AssertionError("wrong rows")));
    Throwable onlyTransient =
        new MultipleFailureException(
            List.of(new RuntimeException("job failed"), new IOException("cleanup failed")));

    assertThat(TransientFailureClassifier.isGenuine(genuineAndTransient)).isTrue();
    assertThat(TransientFailureClassifier.isGenuine(onlyTransient)).isFalse();
  }

  @Test
  public void multipleFailuresInCauseChain_genuineIfAnyIsGenuine() {
    Throwable genuineAndTransient =
        new RuntimeException(
            "wrapper",
            new MultipleFailureException(
                List.of(
                    new RuntimeException("Datastream: unable to queue the operation"),
                    new AssertionError("wrong rows"))));
    Throwable onlyTransient =
        new RuntimeException(
            "wrapper",
            new MultipleFailureException(
                List.of(
                    new RuntimeException("Datastream: unable to queue the operation"),
                    new IOException("cleanup failed"))));

    assertThat(TransientFailureClassifier.isGenuine(genuineAndTransient)).isTrue();
    assertThat(TransientFailureClassifier.isGenuine(onlyTransient)).isFalse();
    assertThat(TransientFailureClassifier.matchingRetryablePattern(onlyTransient).pattern())
        .isEqualTo("(?i)unable to queue");
  }

  @Test
  public void cyclicCauseChain_doesNotLoop() {
    RuntimeException first = new RuntimeException("first");
    RuntimeException second = new RuntimeException("second");
    first.initCause(second);
    second.initCause(first);

    assertThat(TransientFailureClassifier.isGenuine(first)).isFalse();
    assertThat(TransientFailureClassifier.matchingRetryablePattern(first)).isNull();
  }
}
