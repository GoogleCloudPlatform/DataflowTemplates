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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Test;

public class WatchdogRunnableTest {

  private WatchdogRunnable watchdogRunnable;
  private AtomicLong transactionAttemptCount;
  private AtomicBoolean isInTransaction;
  private AtomicBoolean keepWatchdogRunning;

  @Before
  public void setUp() {
    transactionAttemptCount = new AtomicLong(0);
    isInTransaction = new AtomicBoolean(false);
    keepWatchdogRunning = new AtomicBoolean(true);
    watchdogRunnable =
        new WatchdogRunnable(transactionAttemptCount, isInTransaction, keepWatchdogRunning);
    watchdogRunnable.setSleepDuration(2);
  }

  @Test
  public void testTransactionInProgress_noExit() throws InterruptedException {
    // Simulate that a transaction is in progress and changing
    isInTransaction.set(true);
    transactionAttemptCount.set(1);

    // Run watchdog in a separate thread
    Thread watchdogThread = new Thread(watchdogRunnable);
    watchdogThread.start();

    Thread.sleep(1000);
    // Simulate progress in the transaction
    transactionAttemptCount.set(2);

    // Allow some time for the watchdog to run and detect progress
    Thread.sleep(1000);

    // Stop the watchdog
    keepWatchdogRunning.set(false);
    watchdogThread.join();
  }

  @Test
  public void testNoTransaction_noExit() throws InterruptedException {
    // Simulate no transaction is in progress
    isInTransaction.set(false);

    // Run watchdog in a separate thread
    Thread watchdogThread = new Thread(watchdogRunnable);
    watchdogThread.start();

    // Allow some time for the watchdog to run and detect no transaction
    Thread.sleep(3000);

    // Stop the watchdog
    keepWatchdogRunning.set(false);
    watchdogThread.join();
  }
}
