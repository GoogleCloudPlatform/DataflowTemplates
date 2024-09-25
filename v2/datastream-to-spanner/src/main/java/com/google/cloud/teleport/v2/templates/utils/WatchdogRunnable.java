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

import com.google.common.util.concurrent.Uninterruptibles;
import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * The WatchdogRunnable is designed to track if a transaction is making progress by comparing
 * the number of transaction attempts (`transactionAttemptCount`) over time. The `isInTransaction`
 * flag indicates whether a transaction is currently active. If the number of attempts remains
 * the same for a period of 15 minutes while the transaction is active, the watchdog logs a warning
 * and terminates the process by calling `System.exit(1)`.
 */
public class WatchdogRunnable implements Runnable, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(WatchdogRunnable.class);
  private final AtomicLong transactionAttemptCount;
  private final AtomicBoolean isInTransaction;
  private final AtomicBoolean keepWatchdogRunning;
  private transient long sleepDurationInSeconds = 15 * 60;

  public WatchdogRunnable(
      AtomicLong transactionAttemptCount,
      AtomicBoolean isInTransaction,
      AtomicBoolean keepWatchdogRunning) {
    this.transactionAttemptCount = transactionAttemptCount;
    this.isInTransaction = isInTransaction;
    this.keepWatchdogRunning = keepWatchdogRunning;
  }

  @Override
  public void run() {
    long lastTransactionCount = -1;
    while (keepWatchdogRunning.get()) {
      if (isInTransaction.get()) {
        long currentTransactionCount = transactionAttemptCount.get();
        if (lastTransactionCount == currentTransactionCount) {
          LOG.warn(
              "Transaction is not making progress after %s seconds. Terminating process",
              sleepDurationInSeconds);
          System.exit(1);
        }
        lastTransactionCount = currentTransactionCount;
      }
      Uninterruptibles.sleepUninterruptibly(Duration.ofSeconds(sleepDurationInSeconds));
    }
  }

  public void setSleepDuration(long sleepDurationInSeconds) {
    this.sleepDurationInSeconds = sleepDurationInSeconds;
  }
}
