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
import com.google.cloud.teleport.v2.templates.common.Schema;
import com.google.cloud.teleport.v2.templates.dao.DaoFactory;
import com.google.cloud.teleport.v2.templates.dao.MySqlDao;
import com.google.cloud.teleport.v2.templates.kafka.Consumer;
import com.google.cloud.teleport.v2.templates.utils.DMLGenerator;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to read from Kafka and write to Source. */
public class KafkaToSourceStreamer extends DoFn<ProcessingContext, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaToSourceStreamer.class);

  public KafkaToSourceStreamer() {}

  @ProcessElement
  public void processElement(ProcessContext c) {

    ProcessingContext taskContext = c.element();

    LOG.debug(" Worker context: {}", taskContext);

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
        LOG.debug("Number of records read from Kafka: " + kafkaRecords.size());
        if (kafkaRecords.isEmpty()) {
          Thread.sleep(1000); // sleep for 1 sec if there are no records
          continue;
        }

        Schema sourceSchema = taskContext.getSchema();
        List<String> dmlBatch = new ArrayList<>();
        for (String s : kafkaRecords) {
          List<String> parsedValues = parseRecord(s);
          String tableName = parsedValues.get(0);
          String keysJsonStr = parsedValues.get(1);
          String newValuesJsonStr = parsedValues.get(2);
          String modType = parsedValues.get(3);

          LOG.debug(
              "The parsed record is :"
                  + tableName
                  + " key: "
                  + keysJsonStr
                  + " values: "
                  + newValuesJsonStr
                  + " modType: "
                  + modType);

          JSONObject newValuesJson = new JSONObject(newValuesJsonStr);
          JSONObject keysJson = new JSONObject(keysJsonStr);

          String dmlStatment =
              DMLGenerator.getDMLStatement(
                  modType, tableName, sourceSchema, newValuesJson, keysJson);
          dmlBatch.add(dmlStatment);
        }

        dao.batchWrite(dmlBatch);
        kafkaConsumer.commitOffsets();
        LOG.debug(" Successfully processed ");
      } catch (Exception e) {
        LOG.error("The exception is: " + e);
        break;
      }
    }
  }

  private List<String> parseRecord(String rec) {
    LOG.debug("The record to parse: " + rec);
    List<String> response = new ArrayList<>();

    String tablesub = rec.substring(rec.indexOf("tableName=") + 10);

    String tablestr = tablesub.substring(1, tablesub.indexOf(",") - 1);
    response.add(tablestr);
    String keysub = tablesub.substring(tablesub.indexOf("keysJson=") + 9);

    String keystr = keysub.substring(0, keysub.indexOf(", oldValuesJson"));
    response.add(keystr);
    String newValueSub = keysub.substring(keysub.indexOf("newValuesJson=") + 15);

    String newValuesJsonStr = newValueSub.substring(0, newValueSub.indexOf("'}], modType"));
    response.add(newValuesJsonStr);
    String modvaluesub = newValueSub.substring(newValueSub.indexOf("'}], modType=") + 13);

    String modvaluestr = modvaluesub.substring(0, modvaluesub.indexOf(", valueCaptureType="));
    response.add(modvaluestr);

    return response;
  }
}
