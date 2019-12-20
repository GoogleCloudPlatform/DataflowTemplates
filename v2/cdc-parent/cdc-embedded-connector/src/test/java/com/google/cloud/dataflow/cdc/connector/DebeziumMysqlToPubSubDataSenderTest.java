/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.cdc.connector;

import com.google.cloud.dataflow.cdc.common.DataCatalogSchemaUtils;
import com.google.common.collect.Sets;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.util.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumMysqlToPubSubDataSenderTest {

  private static final Logger LOG = LoggerFactory.getLogger(
      DebeziumMysqlToPubSubDataSenderTest.class);

  private static final String GCP_PROJECT = "myproject";

  private static final String PUBSUB_PREFIX = "prefix_";

  private static final String TABLES = "mainstance.cdcfordataflow.team_metadata";

  @Test
  public void testAccurateTermination() {

  }

  private void runEmbeddedEngine() {
      final PubSubChangeConsumer changeConsumer = new PubSubChangeConsumer(
          GCP_PROJECT,
          PUBSUB_PREFIX,
          Sets.newHashSet(TABLES),
          new DataCatalogSchemaUtils(),
          PubSubChangeConsumer.DEFAULT_PUBLISHER_FACTORY);

      final EmbeddedEngine engine = EmbeddedEngine.create()
          //.using(config)
          .using(this.getClass().getClassLoader())
          .using(Clock.SYSTEM)
          .notifying(changeConsumer)
          .build();

      LOG.info("Initializing Debezium Embedded Engine");
      ExecutorService executor = Executors.newSingleThreadExecutor();
      Future<?> future = executor.submit(engine);

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        LOG.info("Requesting embedded engine to shut down");
        engine.stop();
      }));

    try {
      while (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
        LOG.debug("Waiting another 30 seconds for the embedded engine to shut down");
      }
    } catch (InterruptedException e) {
      Thread.interrupted();
    }
  }

}
