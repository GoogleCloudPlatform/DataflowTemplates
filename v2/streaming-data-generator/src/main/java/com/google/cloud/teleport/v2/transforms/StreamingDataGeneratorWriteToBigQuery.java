/*
 * Copyright (C) 2020 Google LLC
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
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;

/**
 * A {@link PTransform} converts fakeMessages to BigQuery Table Rows and loads to BQ. Errors were
 * written to Dead Letter Table.
 */
@AutoValue
public abstract class StreamingDataGeneratorWriteToBigQuery
    extends PTransform<PCollection<byte[]>, PDone> {
  /** Coder for FailsafeElement. */
  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

  /** The default suffix for error tables if dead letter table is not specified. */
  private static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = "_error_records";

  abstract StreamingDataGenerator.StreamingDataGeneratorOptions getPipelineOptions();

  public static Builder builder(StreamingDataGenerator.StreamingDataGeneratorOptions options) {
    return new AutoValue_StreamingDataGeneratorWriteToBigQuery.Builder()
        .setPipelineOptions(options);
  }

  /** Builder for {@link StreamingDataGeneratorBigQueryWriter}. */
  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setPipelineOptions(StreamingDataGenerator.StreamingDataGeneratorOptions value);

    public abstract StreamingDataGeneratorWriteToBigQuery build();
  }

  @Override
  public PDone expand(PCollection<byte[]> fakeMessages) {
    WriteResult writeResults =
        fakeMessages.apply(
            "Write Json messsages",
            BigQueryIO.<byte[]>write()
                .to(getPipelineOptions().getOutputTableSpec())
                .withMethod(Method.STREAMING_INSERTS)
                .ignoreInsertIds()
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(
                    WriteDisposition.valueOf(getPipelineOptions().getWriteDisposition()))
                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                .withExtendedErrorInfo()
                .withFormatFunction(
                    (message) -> {
                      TableRow row = null;
                      try {
                        row =
                            TableRowJsonCoder.of()
                                .decode(new ByteArrayInputStream(message), Coder.Context.OUTER);
                      } catch (IOException e) {
                        throw new RuntimeException(
                            "Failed converting to TableRow with an error:" + e.getMessage());
                      }
                      return row;
                    }));

    // Write errors to Dead Letter table
    writeResults
        .getFailedInsertsWithErr()
        .apply(
            "Convert to FailSafe Element",
            MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                .via(BigQueryConverters::wrapBigQueryInsertError))
        .setCoder(FAILSAFE_ELEMENT_CODER)
        .apply(
            "Write Failed Records",
            ErrorConverters.WriteStringMessageErrors.newBuilder()
                .setErrorRecordsTable(
                    MoreObjects.firstNonNull(
                        getPipelineOptions().getOutputDeadletterTable(),
                        getPipelineOptions().getOutputTableSpec()
                            + DEFAULT_DEADLETTER_TABLE_SUFFIX))
                .setErrorRecordsTableSchema(
                    SchemaUtils.DEADLETTER_SCHEMA) // i.e schema in above method
                .build());
    return PDone.in(fakeMessages.getPipeline());
  }
}
