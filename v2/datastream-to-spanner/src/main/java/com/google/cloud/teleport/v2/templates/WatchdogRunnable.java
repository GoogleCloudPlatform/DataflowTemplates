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
package com.google.cloud.teleport.v2.templates;

import com.google.common.util.concurrent.Uninterruptibles;
import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatchdogRunnable implements Runnable, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(WatchdogRunnable.class);
  private final AtomicLong transactionAttemptCount;
  private final AtomicBoolean isInTransaction;
  private final AtomicBoolean keepWatchdogRunning;

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
          LOG.warn("Transaction is not making progress after 15 minutes. Terminating process");
          System.exit(1);
        }
        lastTransactionCount = currentTransactionCount;
      }
      Uninterruptibles.sleepUninterruptibly(Duration.ofMinutes(15));
    }
  }
}
