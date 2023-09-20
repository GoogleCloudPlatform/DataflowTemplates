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
package com.google.cloud.teleport.v2.auto.blocks;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.auto.value.AutoValue.Builder;
import com.google.cloud.teleport.v2.auto.blocks.WriteToBigQueryTransformProvider.WriteToBigQueryTransformConfiguration;
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.values.PCollectionTuple;

@AutoService(SchemaTransformProvider.class)
public class WriteToBigQueryTransformProvider
    extends TypedSchemaTransformProvider<WriteToBigQueryTransformConfiguration> {
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract class WriteToBigQueryTransformConfiguration {

    String getOutputTableSpec();

    public static Builder builder() {
      return new AutoValue_WriteToBigQueryTransformConfiguration.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setOutputTableSpec(String input);

      public abstract WriteToBigQueryTransformConfiguration build();
    }
  }

  @Override
  public Class<WriteToBigQueryTransformConfiguration> configurationClass() {
    return WriteToBigQueryTransformConfiguration.class;
  }

  @Override
  public SchemaTransform from(WriteToBigQueryTransformConfiguration configuration) {
    return new SchemaTransform() {
      @Override
      public PCollectionTuple expand(PCollectionTuple input) {
        WriteResult writeResult =
            input
                .get("output")
                .apply(
                    "WriteTableRows",
                    BigQueryIO.writeTableRows()
                        .withoutValidation()
                        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                        .withExtendedErrorInfo()
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                        .to(configuration.getOutputTableSpec()));

        return PCollectionTuple.empty(input.getPipeline());
      }
    };
  }

  @Override
  public String identifier() {
    return "blocks:schematransform:org.apache.beam:write_to_bigquery:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Arrays.asList("output");
  }
}
