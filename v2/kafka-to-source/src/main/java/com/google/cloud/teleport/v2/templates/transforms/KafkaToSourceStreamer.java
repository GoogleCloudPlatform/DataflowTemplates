/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import com.google.cloud.teleport.v2.templates.dao.DaoFactory;
import com.google.cloud.teleport.v2.templates.dao.MySqlDao;
import com.google.cloud.teleport.v2.templates.kafka.Consumer;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to read from Kafka and write to Source. */
public class KafkaToSourceStreamer extends DoFn<ProcessingContext, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaToSourceStreamer.class);

  public KafkaToSourceStreamer() {}

  @ProcessElement
  public void processElement(ProcessContext c) {

    ProcessingContext taskContext = c.element();

    LOG.info(" Worker context: " + taskContext.toString());

    String connectString =
        "jdbc:mysql://"
            + taskContext.getShard().getHost()
            + ":"
            + taskContext.getShard().getPort()
            + "/"
            + taskContext.getShard().getDbName();
    Consumer kafkaConsumer =
        new Consumer(
            taskContext.getKafkaConnectionProfile().getDataTopic(),
            taskContext.getKafkaConnectionProfile().getPartitionId(),
            taskContext.getKafkaConnectionProfile().getBootstrapServer());
    MySqlDao dao =
        new DaoFactory(
                connectString,
                taskContext.getShard().getUserName(),
                taskContext.getShard().getPassword())
            .getMySqlDao();

    while (true) {
      try {
        List<String> kafkaRecords = kafkaConsumer.getRecords();
        LOG.info("Number of records read from Kafka: " + kafkaRecords.size());
        if (kafkaRecords.isEmpty()) {
          Thread.sleep(1000); // sleep for 1 sec if there are no records
          continue;
        }

        dao.batchWrite(kafkaRecords);
        kafkaConsumer.commitOffsets();
        LOG.info(" Successfully processed ");
      } catch (Exception e) {
        LOG.error("The exception is: " + e);
        break;
      }
    }
  }
}
