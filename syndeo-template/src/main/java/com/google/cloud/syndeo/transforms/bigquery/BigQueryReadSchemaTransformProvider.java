/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.syndeo.transforms.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

@AutoService({SchemaTransformProvider.class})
public class BigQueryReadSchemaTransformProvider implements SchemaTransformProvider {
  private static final HashMap<String, CreateDisposition> createDispositionsMap = new HashMap();

  public String identifier() {
    return "bigquery:read";
  }

  public Schema configurationSchema() {
    return Schema.builder()
        .addNullableField("table", FieldType.STRING)
        .addNullableField("query", FieldType.STRING)
        .addNullableField("queryLocation", FieldType.STRING)
        .addNullableField("createDisposition", FieldType.STRING)
        .build();
  }

  @Override
  public SchemaTransform from(Row configuration) {
    return new BigQueryReadSchemaTransform(configuration);
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList("OUTPUT");
  }

  static {
    createDispositionsMap.put("Never", CreateDisposition.CREATE_NEVER);
    createDispositionsMap.put("IfNeeded", CreateDisposition.CREATE_IF_NEEDED);
  }

  static class BigQueryReadSchemaTransform implements SchemaTransform {
    protected final Row config;

    BigQueryReadSchemaTransform(Row config) {
      this.config = config;
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
        public PCollectionRowTuple expand(PCollectionRowTuple input) {
          TypedRead<TableRow> read = BigQueryIO.readTableRowsWithSchema();
          read = read.withMethod(Method.EXPORT);
          String table = BigQueryReadSchemaTransform.this.config.getString("table");
          if (table != null) {
            read = read.from(table);
          }

          String query = BigQueryReadSchemaTransform.this.config.getString("query");
          if (query != null) {
            read = read.fromQuery(query).usingStandardSql();
          }

          String queryLocation = BigQueryReadSchemaTransform.this.config.getString("queryLocation");
          if (queryLocation != null) {
            read = read.withQueryLocation(queryLocation);
          }

          if (BigQuerySyndeoServices.servicesVariable != null) {
            read = read.withTestServices(BigQuerySyndeoServices.servicesVariable);
          }

          return PCollectionRowTuple.of(
              "OUTPUT", input.getPipeline().apply(read).apply(Convert.toRows()));
        }
      };
    }
  }
}
