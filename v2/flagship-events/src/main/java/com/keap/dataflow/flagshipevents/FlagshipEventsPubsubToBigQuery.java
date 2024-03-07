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
package com.keap.dataflow.flagshipevents;

import com.google.api.services.bigquery.model.TableRow;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Apache Beam streaming pipeline that reads JSON encoded messages from Pub/Sub and writes the
 * results to one of several BigQuery tables.
 */
public class FlagshipEventsPubsubToBigQuery {
  private static final Logger LOG = LoggerFactory.getLogger(FlagshipEventsPubsubToBigQuery.class);

  public static void main(final String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);

    final String env = options.getEnv();
    final String project = options.as(GcpOptions.class).getProject();
    final String projectAndDataset = String.format("%s:crm_prod", project);
    final String topic = "projects/is-flagship-events-" + env + "/topics/v1.segment-events-core";

    var pipeline = Pipeline.create(options);

    PCollectionList<TableRow> partitionedTableRows =
        pipeline
            .apply("Read messages from Pub/Sub", PubsubIO.readStrings().fromTopic(topic))
            .apply("Split the ampersand-delimited list of JSON objects", batchJsonToListJson())
            .apply("Flatten the list of lists", Flatten.iterables())
            .apply(
                "Parse JSON into TableRows",
                MapElements.via(
                    new SimpleFunction<String, TableRow>() {
                      @Override
                      public TableRow apply(String json) {
                        TableRow tableRow = new TableRow();
                        try {
                          InputStream inputStream =
                              new ByteArrayInputStream(
                                  json.getBytes(StandardCharsets.UTF_8.name()));
                          tableRow =
                              TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
                        } catch (UnsupportedEncodingException e) {
                          LOG.error("Unsupported encoding", e);
                        } catch (IOException e) {
                          LOG.error("Unable to parse input", e);
                        } catch (Exception e) {
                          LOG.error("Error in the JsonToTableRow step", e);
                          throw e;
                        }
                        return tableRow;
                      }
                    }))
            .apply(
                "Split the PCollection by the table that each is TableRow is being written to",
                Partition.of(
                    TableName.values().length,
                    new Partition.PartitionFn<TableRow>() {
                      public int partitionFor(TableRow tableRow, int numPartitions) {
                        TableName tableFromRow = getTableFromRow(tableRow);
                        if (tableFromRow == TableName.UNDEFINED) {
                          LOG.error(String.format("Table UNDEFINED: %s", tableRow.get("event")));
                        }
                        return tableFromRow.index;
                      }
                    }));

    Arrays.stream(TableName.values())
        .sequential()
        .forEach(
            tableName -> {
              partitionedTableRows
                  .get(tableName.getIndex())
                  .apply(
                      "Write to " + tableName.name(),
                      BigQueryIO.writeTableRows()
                          .to(
                              String.format(
                                  "%s.%s", projectAndDataset, tableName.getFormattedTable()))
                          // .withSchema(tableSchema)
                          .withoutValidation()
                          .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                          .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                          .withExtendedErrorInfo()
                          .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                          .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()))
                  .getFailedInsertsWithErr()
                  .apply(
                      "Write errors",
                      MapElements.via(
                          new SimpleFunction<BigQueryInsertError, String>() {
                            @Override
                            public String apply(BigQueryInsertError x) {

                              try {
                                final String error =
                                    "Failed insert: "
                                        + x.getError().toPrettyString()
                                        + "\nRow: "
                                        + x.getRow().toPrettyString();
                                LOG.error(error);
                                return error;
                              } catch (IOException e) {
                                LOG.error("Failed insert.\nCan't parse row");
                                throw new RuntimeException(e);
                              }
                            }
                          }));
            });

    pipeline.run();
  }

  public static PTransform<PCollection<String>, PCollection<Iterable<String>>>
      batchJsonToListJson() {
    return new BatchJsonToListJson();
  }

  private static class BatchJsonToListJson
      extends PTransform<PCollection<String>, PCollection<Iterable<String>>> {
    @Override
    public PCollection<Iterable<String>> expand(PCollection<String> input) {
      return input.apply(
          "BatchJsonToJsonList",
          MapElements.via(
              new SimpleFunction<>() {
                @Override
                public Iterable<String> apply(String input) {
                  String[] splitInput = input.split("&");
                  Set<String> inputSet = new HashSet<>();

                  for (String string : splitInput) {
                    try {
                      inputSet.add(string);
                    } catch (Exception e) {
                      LOG.error("Unable to parse input in BatchJsonToListJson", e);
                    }
                  }
                  return inputSet;
                }
              }));
    }
  }

  private static TableName getTableFromRow(TableRow tableRow) {
    return TableName.getByTable((String) tableRow.get("event"));
  }

  private enum TableName {
    USER_LOGIN_TABLE(0, "user_login"),
    USER_LOGOUT_TABLE(1, "user_logout"),
    API_CALL_MADE_TABLE(2, "api_call_made"),
    UNDEFINED(3, null);

    private final int index;
    private final String formattedTable;

    TableName(int index, String formattedTable) {
      this.index = index;
      this.formattedTable = formattedTable;
    }

    public int getIndex() {
      return this.index;
    }

    public String getFormattedTable() {
      return this.formattedTable;
    }

    public static TableName getByTable(String table) {
      return Arrays.stream(TableName.values())
          .filter(
              it -> it.getFormattedTable().equals(StringUtils.defaultString(table).toLowerCase()))
          .findFirst()
          .orElse(UNDEFINED);
    }
  }

  public interface Options extends StreamingOptions {
    @Description("Environment deploying into")
    @Validation.Required
    String getEnv();

    void setEnv(String value);
  }
}
