// Copyright 2020 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.keap.dataflow.flagshipevents;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Apache Beam streaming pipeline that reads JSON encoded messages fromPub/Sub,
 * uses Beam SQL to transform the message data, and writes the results to a BigQuery.
 */
public class FlagshipEventsPubsubToBigQuery {
    private static final Logger LOG = LoggerFactory.getLogger(FlagshipEventsPubsubToBigQuery.class);
    private static final Gson GSON = new Gson();

    public static void main(final String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        options.setStreaming(true);

        final String env = options.getEnv();
        final String project = options.as(GcpOptions.class).getProject();
        final String projectAndDataset = String.format("%s:crm_prod", project);
        final String topic = "projects/is-flagship-events-" + env + "/topics/v1.segment-events-core";

        var pipeline = Pipeline.create(options);

        pipeline
            .apply("Read messages from Pub/Sub", PubsubIO.readStrings().fromTopic(topic))
            .apply("Parse JSON into Maps", MapElements.into(TypeDescriptors.maps(TypeDescriptors.strings(), TypeDescriptors.strings()))
                    .via(message -> GSON.fromJson(message, new TypeToken<Map<String, String>>() {}.getType())))
            .apply("Translate to TableRow and set Schemas", MapElements.into(TypeDescriptor.of(TableContent.class))
                    .via(FlagshipEventsPubsubToBigQuery::buildTableContentFromMap))
            .apply("Write to BigQuery", ParDo.of(new DoFn<TableContent, Void>() {
                @DoFn.ProcessElement
                public void processElement(@DoFn.Element TableContent tableContent, OutputReceiver<Void> out) {
                    BigQueryIO.write()
                            .withFormatFunction(tableContentTableRowSerializableFunction())
                            .to(String.format("%s.%s", projectAndDataset, tableContent.getTableName()))
                            .withSchema(tableContent.getTableSchema())
                            .withoutValidation()
                            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                            .withExtendedErrorInfo()
                            .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                            .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors());
                }
            }));

        pipeline.run();
    }

    private static TableContent buildTableContentFromMap(Map<String, String> map) {
        TableContent tableContent = new TableContent();
        tableContent.setTableName(setTableNameFromMap(map));
        tableContent.setTableRow(buildRowFromMap(map));
        tableContent.setTableSchema(buildSchemaFromMap(map));
        return tableContent;
    }

    private static TableSchema buildSchemaFromMap(Map<String, String> map) {
        TableSchema tableSchema = new TableSchema();
        ArrayList<TableFieldSchema> fieldSchema = new ArrayList<>();
        map.keySet().forEach(it -> fieldSchema.add(new TableFieldSchema().setName(it).setType("STRING")));
        tableSchema.setFields(fieldSchema);
        return tableSchema;
    }

    private static TableRow buildRowFromMap(Map<String, String> map) {
        TableRow tableRow = new TableRow();
        tableRow.putAll(map);
        return tableRow;
    }

    private static String setTableNameFromMap(Map<String, String> map) {
        return tableFromMap(map).getFormattedTable();
    }

    private static TableName tableFromMap(Map<String, String> map) {
        switch (map.get("event")) {
            case "user_login":
                return TableName.USER_LOGIN_TABLE;
            case "user_logout":
                return TableName.USER_LOGOUT_TABLE;
            case "api_call_made":
                return TableName.API_CALL_MADE_TABLE;
            default:
                return TableName.UNEXPECTED_TABLE;
        }
    }

    private static @UnknownKeyFor @Initialized SerializableFunction<@UnknownKeyFor @Nullable @Initialized Object, @UnknownKeyFor @NonNull @Initialized TableRow> tableContentTableRowSerializableFunction() {
        return input -> ((TableContent)input).getTableRow();
    }

    private enum TableName {
        UNEXPECTED_TABLE(0, "unexpected"), USER_LOGIN_TABLE(1, "user_login"), USER_LOGOUT_TABLE(2, "user_logout"), API_CALL_MADE_TABLE(3, "api_call_made");

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

        public TableName get(int index) {
            return Arrays.stream(TableName.values()).filter(it -> it.getIndex() == index).findFirst().orElseThrow();
        }
    }

    public interface Options extends StreamingOptions {
        @Description("Environment deploying into")
        @Validation.Required
        String getEnv();

        void setEnv(String value);
    }

}
