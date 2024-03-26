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
package com.google.cloud.teleport.v2.transformer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventSessionConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventSpannerConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * Transforms a {@link JsonNode} to a {@link Mutation}. Apply transformations based on the schema
 * object in the process.
 */
public class JsonNodeToMutationDoFn extends DoFn<JsonNode, Mutation> implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(JsonNodeToMutationDoFn.class);

    private Schema schema;
    private String srcTable;

    private final Ddl ddl;

    private final Counter mutationBuildErrors =
            Metrics.counter(JsonNodeToMutationDoFn.class, "mutationBuildErrors");

    // Jackson Object mapper.
    private transient ObjectMapper mapper;

    // ChangeEventSessionConvertor utility object.
    private ChangeEventSessionConvertor changeEventSessionConvertor;

    public JsonNodeToMutationDoFn(
            Schema schema, String srcTable, Ddl ddl) {
        this.schema = schema;
        this.srcTable = srcTable;
        this.ddl = ddl;
    }

    /**
     * Setup function connects to Cloud Spanner.
     */
    @Setup
    public void setup() {
        mapper = new ObjectMapper();
        mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        changeEventSessionConvertor = new ChangeEventSessionConvertor(schema);
        changeEventSessionConvertor.shouldGenerateUUID(true);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        // We create a copy because Beam does not allow mutations on the input element (i.e c.element()) for unit tests.
        JsonNode changeEvent = c.element().deepCopy();
        try {
            schema.verifyTableInSession(srcTable);
            changeEvent = changeEventSessionConvertor.transformChangeEventViaSessionFile(changeEvent);
            String spTableName = changeEvent.get(Constants.EVENT_TABLE_NAME_KEY).asText();
            List<String> columnNames = schema.getSpannerColumnNames(spTableName);
            Set<String> keyColumns = schema.getPrimaryKeySet(spTableName);
            Table table = ddl.table(spTableName);

            Mutation mutation = ChangeEventSpannerConvertor.mutationFromEvent(table, changeEvent, columnNames, keyColumns);
            c.output(mutation);
        } catch (Exception ex) {
            mutationBuildErrors.inc();
            LOG.error(ex.toString());
        }
    }
}