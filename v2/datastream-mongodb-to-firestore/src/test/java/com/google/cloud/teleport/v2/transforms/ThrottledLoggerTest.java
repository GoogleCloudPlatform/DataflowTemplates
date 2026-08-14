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
package com.google.cloud.teleport.v2.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ThrottledLogger}. */
@RunWith(JUnit4.class)
public class ThrottledLoggerTest {

  @Test
  public void testBasicRecordingAndGetters() {
    ThrottledLogger logger = new ThrottledLogger("TestComponent", 30_000L);
    logger.recordRetryableError("NETWORK", "Connection reset");
    logger.recordRetryableError("NETWORK", "Timeout");
    logger.recordSevereError("VALIDATION", "Invalid schema");

    assertEquals(3L, logger.getTotalErrors());
    assertEquals(2L, logger.getTotalRetryable());
    assertEquals(1L, logger.getTotalSevere());
  }

  @Test
  public void testFlushSummaryResetsCounts() {
    ThrottledLogger logger = new ThrottledLogger("TestComponent", 30_000L);
    logger.recordRetryableError("NETWORK", "Connection reset");
    logger.recordSevereError("SCHEMA", "Bad field");

    assertEquals(2L, logger.getTotalErrors());
    logger.flushSummary();
    assertEquals(0L, logger.getTotalErrors());
    assertEquals(0L, logger.getTotalRetryable());
    assertEquals(0L, logger.getTotalSevere());
  }

  @Test
  public void testShouldLogThrottling() {
    ThrottledLogger logger = new ThrottledLogger("TestComponent", 5000L);
    assertTrue(logger.shouldLog("key1"));
    assertFalse(logger.shouldLog("key1"));
    assertFalse(logger.shouldLog("key1"));

    long suppressed = logger.getAndResetSuppressedCount("key1");
    assertEquals(2L, suppressed);
    assertEquals(0L, logger.getAndResetSuppressedCount("key1"));
  }

  @Test
  public void testSerializationAndConcurrentAccess() throws Exception {
    ThrottledLogger original = new ThrottledLogger("ConcurrentLogger", 30_000L);

    // Serialize and deserialize to simulate Beam worker distribution where transient fields start
    // null
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(original);
    }

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ThrottledLogger deserialized;
    try (ObjectInputStream ois = new ObjectInputStream(bais)) {
      deserialized = (ThrottledLogger) ois.readObject();
    }

    int threadCount = 20;
    int incrementsPerThread = 1000;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);

    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      executor.submit(
          () -> {
            try {
              for (int j = 0; j < incrementsPerThread; j++) {
                if (threadId % 2 == 0) {
                  deserialized.recordRetryableError("CAT_RETRY", "retry error");
                } else {
                  deserialized.recordSevereError("CAT_SEVERE", "severe error");
                }
              }
            } finally {
              latch.countDown();
            }
          });
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS));
    executor.shutdown();

    long expectedTotal = (long) threadCount * incrementsPerThread;
    assertEquals(expectedTotal, deserialized.getTotalErrors());
    assertEquals(expectedTotal / 2, deserialized.getTotalRetryable());
    assertEquals(expectedTotal / 2, deserialized.getTotalSevere());
  }

  @Test
  public void testNullCategoryAndKeySafety() {
    ThrottledLogger logger = new ThrottledLogger("NullSafetyLogger", 5000L);
    logger.recordRetryableError(null, "Null category message");
    logger.recordSevereError(null, "Null category severe message");

    assertEquals(2L, logger.getTotalErrors());
    assertTrue(logger.shouldLog(null));
    assertFalse(logger.shouldLog(null));
    assertEquals(1L, logger.getAndResetSuppressedCount(null));
  }

  @Test
  public void testLogInfoWarnErrorMethods() {
    ThrottledLogger logger = new ThrottledLogger("LogMethodsLogger", 5000L);
    org.slf4j.Logger mockLogger = org.mockito.Mockito.mock(org.slf4j.Logger.class);

    logger.logInfo(mockLogger, "infoKey", "info message");
    logger.logInfo(mockLogger, "infoKey", "info message 2");
    org.mockito.Mockito.verify(mockLogger, org.mockito.Mockito.times(1))
        .info("info message", new Object[] {});

    logger.logWarn(mockLogger, "warnKey", "warn message");
    logger.logWarn(mockLogger, "warnKey", "warn message 2");
    org.mockito.Mockito.verify(mockLogger, org.mockito.Mockito.times(1))
        .warn("warn message", new Object[] {});

    logger.logError(mockLogger, "errorKey", "error message");
    logger.logError(mockLogger, "errorKey", "error message 2");
    org.mockito.Mockito.verify(mockLogger, org.mockito.Mockito.times(1))
        .error("error message", new Object[] {});
  }

  @Test
  public void testCategoryAndKeyOverflowRoutesToOther() {
    ThrottledLogger logger = new ThrottledLogger("OverflowLogger", 5000L);

    // Register 250 distinct categories (limit is 200)
    for (int i = 0; i < 250; i++) {
      logger.recordRetryableError("CAT_" + i, "error msg " + i);
    }
    assertEquals(250L, logger.getTotalErrors());

    // Register 250 distinct log keys (limit is 200)
    int loggedCount = 0;
    for (int i = 0; i < 250; i++) {
      if (logger.shouldLog("KEY_" + i)) {
        loggedCount++;
      }
    }
    // The first 200 unique keys + 1 for "OTHER" log once; remaining 49 are suppressed under "OTHER"
    assertEquals(201, loggedCount);
  }
}
