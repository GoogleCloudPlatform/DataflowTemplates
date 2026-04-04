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
package com.google.cloud.teleport.v2.neo4j.transforms;

import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.model.helpers.StepSequence;
import com.google.cloud.teleport.v2.neo4j.telemetry.ReportedSourceType;
import com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils;
import com.google.cloud.teleport.v2.neo4j.utils.SerializableSupplier;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.neo4j.importer.v1.Configuration;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.sources.Source;
import org.neo4j.importer.v1.targets.Target;
import org.neo4j.importer.v1.targets.TargetType;

/** Neo4j write transformation. */
public class Neo4jRowWriterTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
  private static final String NODE_BATCH_SIZE_SETTING = "node_target_batch_size";
  private static final String LEGACY_NODE_BATCH_SIZE_SETTING = "node_write_batch_size";
  private static final Integer DEFAULT_NODE_BATCH_SIZE = 5000;
  private static final String RELATIONSHIP_BATCH_SIZE_SETTING = "relationship_target_batch_size";
  private static final String LEGACY_RELATIONSHIP_BATCH_SIZE_SETTING = "edge_write_batch_size";
  private static final Integer DEFAULT_RELATIONSHIP_BATCH_SIZE = 1000;
  private static final String QUERY_BATCH_SIZE_SETTING = "query_target_batch_size";
  private static final String LEGACY_QUERY_BATCH_SIZE_SETTING = "custom_query_batch_size";
  private static final Integer DEFAULT_QUERY_BATCH_SIZE = 1000;

  private static final String NODE_PARALLELISM_SETTING = "node_target_parallelism";
  private static final String LEGACY_NODE_PARALLELISM_SETTING = "node_write_parallelism";
  private static final Integer DEFAULT_NODE_PARALLELISM_FACTOR = 5;
  private static final String RELATIONSHIP_PARALLELISM_SETTING = "relationship_target_parallelism";
  private static final String LEGACY_RELATIONSHIP_PARALLELISM_SETTING = "edge_write_parallelism";
  private static final Integer DEFAULT_RELATIONSHIP_PARALLELISM_FACTOR = 1;
  private static final String QUERY_PARALLELISM_SETTING = "query_target_parallelism";
  private static final String LEGACY_QUERY_PARALLELISM_SETTING = "custom_query_parallelism";
  private static final Integer DEFAULT_QUERY_PARALLELISM_FACTOR = 1;

  private final ImportSpecification importSpecification;
  private final Target target;
  private final SerializableSupplier<Neo4jConnection> connectionSupplier;
  private final StepSequence targetSequence;

  public Neo4jRowWriterTransform(
      ImportSpecification importSpecification,
      ConnectionParams neoConnection,
      String templateVersion,
      StepSequence targetSequence,
      Target target) {
    this(
        importSpecification,
        targetSequence,
        target,
        () -> new Neo4jConnection(neoConnection, templateVersion));
  }

  @VisibleForTesting
  Neo4jRowWriterTransform(
      ImportSpecification importSpecification,
      StepSequence targetSequence,
      Target target,
      SerializableSupplier<Neo4jConnection> connectionSupplier) {
    this.importSpecification = importSpecification;
    this.target = target;
    this.connectionSupplier = connectionSupplier;
    this.targetSequence = targetSequence;
  }

  @NonNull
  @Override
  public PCollection<Row> expand(@NonNull PCollection<Row> input) {
    var targetType = target.getTargetType();
    ReportedSourceType reportedSourceType = determineReportedSourceType();

    Configuration config = importSpecification.getConfiguration();

    Neo4jBlockingUnwindFn neo4jUnwindFn =
        new Neo4jBlockingUnwindFn(
            reportedSourceType,
            importSpecification,
            target,
            false,
            "rows",
            getRowCastingFunction(),
            connectionSupplier);

    PCollection<Row> readyInput = input;
    if (targetType == TargetType.NODE || targetType == TargetType.RELATIONSHIP) {
      var schemaSetupDone =
          input
              .getPipeline()
              .apply("Schema setup seed " + target.getName(), Create.of(1))
              .apply(
                  "Schema setup " + target.getName(),
                  ParDo.of(new Neo4jInitSchemaFn(target, reportedSourceType, connectionSupplier)));
      readyInput =
          input
              .apply("Wait for schema setup " + target.getName(), Wait.on(schemaSetupDone))
              .setCoder(input.getCoder());
    }

    return readyInput
        .apply(
            "Create KV pairs",
            WithKeys.of(ThreadLocalRandomInt.of(parallelismFactor(targetType, config))))
        .apply("Group into batches", GroupIntoBatches.ofSize(batchSize(targetType, config)))
        .apply(
            targetSequence.getSequenceNumber(target) + ": Neo4j write " + target.getName(),
            ParDo.of(neo4jUnwindFn))
        .setRowSchema(readyInput.getSchema());
  }

  private ReportedSourceType determineReportedSourceType() {
    Source source =
        importSpecification.getSources().stream()
            .filter(src -> src.getName().equals(target.getSource()))
            .findFirst()
            .get();
    return ReportedSourceType.reportedSourceTypeOf(source);
  }

  private SerializableFunction<Row, Map<String, Object>> getRowCastingFunction() {
    return (row) -> DataCastingUtils.rowToNeo4jDataMap(row, target);
  }

  private static int batchSize(TargetType targetType, Configuration config) {
    return switch (targetType) {
      case NODE -> config
          .get(Integer.class, NODE_BATCH_SIZE_SETTING, LEGACY_NODE_BATCH_SIZE_SETTING)
          .orElse(DEFAULT_NODE_BATCH_SIZE);
      case RELATIONSHIP -> config
          .get(
              Integer.class,
              RELATIONSHIP_BATCH_SIZE_SETTING,
              LEGACY_RELATIONSHIP_BATCH_SIZE_SETTING)
          .orElse(DEFAULT_RELATIONSHIP_BATCH_SIZE);
      case QUERY -> config
          .get(Integer.class, QUERY_BATCH_SIZE_SETTING, LEGACY_QUERY_BATCH_SIZE_SETTING)
          .orElse(DEFAULT_QUERY_BATCH_SIZE);
    };
  }

  private static int parallelismFactor(TargetType targetType, Configuration config) {
    return switch (targetType) {
      case NODE -> config
          .get(Integer.class, NODE_PARALLELISM_SETTING, LEGACY_NODE_PARALLELISM_SETTING)
          .orElse(DEFAULT_NODE_PARALLELISM_FACTOR);
      case RELATIONSHIP -> config
          .get(
              Integer.class,
              RELATIONSHIP_PARALLELISM_SETTING,
              LEGACY_RELATIONSHIP_PARALLELISM_SETTING)
          .orElse(DEFAULT_RELATIONSHIP_PARALLELISM_FACTOR);
      case QUERY -> config
          .get(Integer.class, QUERY_PARALLELISM_SETTING, LEGACY_QUERY_PARALLELISM_SETTING)
          .orElse(DEFAULT_QUERY_PARALLELISM_FACTOR);
    };
  }

  private record ThreadLocalRandomInt(int bound) implements SerializableFunction<Row, Integer> {

    public static SerializableFunction<Row, Integer> of(int bound) {
      return new ThreadLocalRandomInt(bound);
    }

    @Override
    public Integer apply(Row input) {
      return ThreadLocalRandom.current().nextInt(bound);
    }
  }
}
