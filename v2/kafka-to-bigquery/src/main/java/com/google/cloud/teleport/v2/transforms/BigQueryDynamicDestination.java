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
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;

public class BigQueryDynamicDestination
    extends DynamicDestinations<KV<GenericRecord, TableRow>, GenericRecord> {

  private String projectName;

  private String datasetName;

  private String tableNamePrefix;

  public static BigQueryDynamicDestination of(
      String projectName, String datasetName, String tableNamePrefix) {
    return new BigQueryDynamicDestination(projectName, datasetName, tableNamePrefix);
  }

  private BigQueryDynamicDestination(
      String projectName, String datasetName, String tableNamePrefix) {
    this.projectName = projectName;
    this.datasetName = datasetName;
    this.tableNamePrefix = tableNamePrefix;
  }

  @Override
  public GenericRecord getDestination(ValueInSingleWindow<KV<GenericRecord, TableRow>> element) {
    return element.getValue().getKey();
  }

  @Override
  public TableDestination getTable(GenericRecord element) {
    // tablename + record name (same across schemas) + schema id?
    String bqQualifiedFullName = element.getSchema().getFullName().replace(".", "-");
    String tableName =
        this.tableNamePrefix + (this.tableNamePrefix == "" ? "" : "-") + bqQualifiedFullName;
    String tableSpec = this.projectName + ":" + this.datasetName + "." + tableName;
    return new TableDestination(tableSpec, null);
  }

  @Override
  public TableSchema getSchema(GenericRecord element) {
    // TODO: Test if sending null can work here, might be mroe efficient.
    return BigQueryUtils.toTableSchema(AvroUtils.toBeamSchema(element.getSchema()));
  }

  @Override
  public Coder<GenericRecord> getDestinationCoder() {
    return GenericRecordCoder.of();
  }
}
