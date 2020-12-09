/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.transforms.io;

import com.google.bigtable.v2.Mutation;
import com.google.cloud.teleport.v2.options.ProtegrityDataTokenizationOptions;
import com.google.protobuf.ByteString;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link BigTableIO} class for writing data from template to BigTable.
 */
public class BigTableIO {
    private static final Logger LOG = LoggerFactory.getLogger(BigTableIO.class);

    private final ProtegrityDataTokenizationOptions options;

    public BigTableIO(ProtegrityDataTokenizationOptions options) {
        this.options = options;
    }

    public PDone write(
                PCollection<Row> input,
            Schema schema
    ) {
        return input.apply("convertToBigTableFormat", ParDo.of(new TransformToBigTableFormat(schema)))
                .apply("writeToBigTable", BigtableIO.write()
                        .withProjectId(options.getBigTableProjectId())
                        .withInstanceId(options.getBigTableInstanceId())
                        .withTableId(options.getBigTableTableId())) ;
    }

    static class TransformToBigTableFormat extends DoFn<Row, KV<ByteString, Iterable<Mutation>>> {

        private final Schema schema;

        TransformToBigTableFormat(Schema schema) {
            this.schema = schema;
        }

        @ProcessElement
        public void processElement(@Element Row in, OutputReceiver<KV<ByteString, Iterable<Mutation>>> out, ProcessContext c) {
            ProtegrityDataTokenizationOptions options = c.getPipelineOptions().as(ProtegrityDataTokenizationOptions.class);
            // Mapping every field in provided Row to Mutation.SetCell, which will create/update
            // cell content with provided data
            Set<Mutation> mutations = schema.getFields().stream()
                    .map(Schema.Field::getName)
                    // Ignoring key field, otherwise it will be added as regular column
                    .filter(fieldName -> !Objects.equals(fieldName, options.getBigTableKeyColumnName()))
                    .map(fieldName -> Pair.of(fieldName, in.getString(fieldName)))
                    .map(pair ->
                            Mutation.newBuilder()
                                    .setSetCell(
                                            Mutation.SetCell.newBuilder()
                                                    .setFamilyName(options.getBigTableColumnFamilyName())
                                                    .setColumnQualifier(ByteString.copyFrom(pair.getKey().getBytes()))
                                                    .setValue(ByteString.copyFrom(pair.getValue().getBytes()))
                                                    .setTimestampMicros(System.currentTimeMillis() * 1000)
                                                    .build()
                                    )
                                    .build()
                    )
                    .collect(Collectors.toSet());
            // Converting key value to BigTable format
            //TODO ramazan@akvelon.com check that please (NPE)
            ByteString key = ByteString.copyFrom(Objects.requireNonNull(in.getString(options.getBigTableKeyColumnName())).getBytes());
            out.output(KV.of(key, mutations));
        }
    }
}
