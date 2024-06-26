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
package com.google.cloud.teleport.v2.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.v2.coders.GenericRecordCoder;
import com.google.cloud.teleport.v2.utils.BigQueryAvroUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;

public class BigQueryDynamicDestination
    extends DynamicDestinations<KV<GenericRecord, TableRow>, GenericRecord> {

  private String projectName;

  private String datasetName;

  private String tableNamePrefix;

  private boolean persistKafkaKey;

  public static BigQueryDynamicDestination of(
      String projectName, String datasetName, String tableNamePrefix, boolean persistKafkaKey) {
    return new BigQueryDynamicDestination(
        projectName, datasetName, tableNamePrefix, persistKafkaKey);
  }

  private BigQueryDynamicDestination(
      String projectName, String datasetName, String tableNamePrefix, boolean persistKafkaKey) {
    this.projectName = projectName;
    this.datasetName = datasetName;
    this.tableNamePrefix = tableNamePrefix;
    this.persistKafkaKey = persistKafkaKey;
  }

  @Override
  public GenericRecord getDestination(ValueInSingleWindow<KV<GenericRecord, TableRow>> element) {
    return element.getValue().getKey();
  }

  @Override
  public TableDestination getTable(GenericRecord element) {
    String sanitizedNamespace =
        BigQueryAvroUtils.sanitizeString(element.getSchema().getNamespace());
    String sanitizedName = BigQueryAvroUtils.sanitizeString(element.getSchema().getName());

    String bqQualifiedFullName =
        sanitizedNamespace + (sanitizedNamespace.isBlank() ? "" : "-") + sanitizedName;

    String tableName =
        this.tableNamePrefix + (this.tableNamePrefix.isBlank() ? "" : "-") + bqQualifiedFullName;
    String tableSpec = this.projectName + ":" + this.datasetName + "." + tableName;
    return new TableDestination(tableSpec, null);
  }

  @Override
  public TableSchema getSchema(GenericRecord element) {
    // TODO: Test if sending null can work here, might be more efficient.
    return BigQueryAvroUtils.convertAvroSchemaToTableSchema(
        element.getSchema(), this.persistKafkaKey);
  }

  @Override
  public Coder<GenericRecord> getDestinationCoder() {
    return GenericRecordCoder.of();
  }
}
