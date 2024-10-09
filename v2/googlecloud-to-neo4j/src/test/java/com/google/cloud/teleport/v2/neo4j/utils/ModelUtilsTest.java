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
package com.google.cloud.teleport.v2.neo4j.utils;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec.TargetQuerySpecBuilder;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.importer.v1.targets.Aggregation;
import org.neo4j.importer.v1.targets.NodeKeyConstraint;
import org.neo4j.importer.v1.targets.NodeMatchMode;
import org.neo4j.importer.v1.targets.NodeSchema;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.RelationshipKeyConstraint;
import org.neo4j.importer.v1.targets.RelationshipSchema;
import org.neo4j.importer.v1.targets.RelationshipTarget;
import org.neo4j.importer.v1.targets.SourceTransformations;
import org.neo4j.importer.v1.targets.Target;
import org.neo4j.importer.v1.targets.WriteMode;

public class ModelUtilsTest {

  @Test
  public void should_generate_correct_sql_statement_for_nodes_no_keys() {
    assertThat(
            ModelUtils.getTargetSql(
                specForNode(
                        "Person",
                        Map.of(
                            "ID",
                            "id",
                            "NAME",
                            "name",
                            "SURNAME",
                            "surname",
                            "DATE_OF_BIRTH",
                            "dob"),
                        List.of())
                    .getTarget(),
                null,
                null,
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                false,
                null))
        .isEqualTo("SELECT * FROM PCOLLECTION");
  }

  @Test
  public void should_generate_correct_sql_statement_for_nodes_with_keys() {
    assertThat(
            ModelUtils.getTargetSql(
                specForNode(
                        "Person",
                        Map.of(
                            "ID",
                            "id",
                            "NAME",
                            "name",
                            "SURNAME",
                            "surname",
                            "DATE_OF_BIRTH",
                            "dob"),
                        List.of("ID"))
                    .getTarget(),
                null,
                null,
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                false,
                null))
        .isEqualTo("SELECT * FROM PCOLLECTION");
  }

  @Test
  public void should_generate_correct_sql_statement_for_nodes_with_base_sql() {
    assertThat(
            ModelUtils.getTargetSql(
                specForNode(
                        "Person",
                        Map.of(
                            "ID",
                            "id",
                            "NAME",
                            "name",
                            "SURNAME",
                            "surname",
                            "DATE_OF_BIRTH",
                            "dob"),
                        List.of("ID"))
                    .getTarget(),
                null,
                null,
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                false,
                "SELECT ID, NAME, SURNAME, DATE_OF_BIRTH, BIRTH_PLACE FROM TABLE"))
        .isEqualTo(
            "SELECT * FROM (SELECT ID, NAME, SURNAME, DATE_OF_BIRTH, BIRTH_PLACE FROM TABLE)");
  }

  @Test
  public void should_generate_correct_sql_statement_for_nodes_with_base_sql_sort() {
    assertThat(
            ModelUtils.getTargetSql(
                specForNode(
                        "Person",
                        Map.of(
                            "ID",
                            "id",
                            "NAME",
                            "name",
                            "SURNAME",
                            "surname",
                            "DATE_OF_BIRTH",
                            "dob"),
                        List.of("ID"))
                    .getTarget(),
                null,
                null,
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                true,
                "SELECT ID, NAME, SURNAME, DATE_OF_BIRTH, BIRTH_PLACE FROM TABLE"))
        .isEqualTo(
            "SELECT * FROM (SELECT ID, NAME, SURNAME, DATE_OF_BIRTH, BIRTH_PLACE FROM TABLE)");
  }

  @Test
  public void should_generate_correct_sql_statement_for_relationships_no_keys() {
    var relSpec =
        specForRel(
            "WORKS_AT",
            Map.of("CONTRACT_DATE", "contractDate"),
            List.of(),
            "Person",
            Map.of("PERSON_ID", "id"),
            "Company",
            Map.of("COMPANY_ID", "id"));
    assertThat(
            ModelUtils.getTargetSql(
                relSpec.getTarget(),
                relSpec.getStartNodeTarget(),
                relSpec.getEndNodeTarget(),
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                false,
                null))
        .isEqualTo("SELECT * FROM PCOLLECTION");
  }

  @Test
  public void should_generate_correct_sql_statement_for_relationships_with_keys() {
    var relSpec =
        specForRel(
            "WORKS_AT",
            Map.of("ID", "id", "CONTRACT_DATE", "contractDate"),
            List.of("ID"),
            "Person",
            Map.of("PERSON_ID", "id"),
            "Company",
            Map.of("COMPANY_ID", "id"));
    assertThat(
            ModelUtils.getTargetSql(
                relSpec.getTarget(),
                relSpec.getStartNodeTarget(),
                relSpec.getEndNodeTarget(),
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                false,
                null))
        .isEqualTo("SELECT * FROM PCOLLECTION");
  }

  @Test
  public void should_generate_correct_sql_statement_for_relationships_with_base_sql() {
    var relSpec =
        specForRel(
            "WORKS_AT",
            Map.of("ID", "id", "CONTRACT_DATE", "contractDate"),
            List.of("ID"),
            "Person",
            Map.of("PERSON_ID", "id"),
            "Company",
            Map.of("COMPANY_ID", "id"));
    assertThat(
            ModelUtils.getTargetSql(
                relSpec.getTarget(),
                relSpec.getStartNodeTarget(),
                relSpec.getEndNodeTarget(),
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                false,
                "SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE"))
        .isEqualTo("SELECT * FROM (SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE)");
  }

  @Test
  public void should_generate_correct_sql_statement_for_relationships_with_base_sql_sort() {
    var relSpec =
        specForRel(
            "WORKS_AT",
            Map.of("ID", "id", "CONTRACT_DATE", "contractDate"),
            List.of("ID"),
            "Person",
            Map.of("PERSON_ID", "id"),
            "Company",
            Map.of("COMPANY_ID", "id"));
    assertThat(
            ModelUtils.getTargetSql(
                relSpec.getTarget(),
                relSpec.getStartNodeTarget(),
                relSpec.getEndNodeTarget(),
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                true,
                "SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE"))
        .isEqualTo(
            "SELECT * FROM (SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE) ORDER BY `COMPANY_ID`");
  }

  @Test
  public void should_generate_correct_sql_statement_for_transforms_with_grouping() {
    final TargetQuerySpec build =
        new TransformBuilder(
                specForNode(
                    "Person",
                    mapOf("ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of()))
            .group(true)
            .build();
    assertThat(
            ModelUtils.getTargetSql(
                build.getTarget(),
                null,
                null,
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                false,
                null))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH` FROM PCOLLECTION GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`");
  }

  @Test
  public void should_generate_correct_sql_statement_for_transforms_with_grouping_limited() {
    var nodeSpec =
        new TransformBuilder(
                specForNode(
                    "Person",
                    mapOf("ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of()))
            .group(true)
            .limit(100)
            .build();
    assertThat(
            ModelUtils.getTargetSql(
                nodeSpec.getTarget(),
                null,
                null,
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                false,
                null))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH` FROM PCOLLECTION GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH` LIMIT 100");
  }

  @Test
  public void should_generate_correct_sql_statement_for_transforms_with_grouping_where() {
    final TargetQuerySpec build =
        new TransformBuilder(
                specForNode(
                    "Person",
                    mapOf("ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of()))
            .group(true)
            .where("NAME LIKE 'A%'")
            .build();
    assertThat(
            ModelUtils.getTargetSql(
                build.getTarget(),
                null,
                null,
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                false,
                null))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH` FROM PCOLLECTION WHERE NAME LIKE 'A%' GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`");
  }

  @Test
  public void should_generate_correct_sql_statement_for_transforms_with_aggregation() {
    final TargetQuerySpec build =
        new TransformBuilder(
                specForNode(
                    "Person",
                    mapOf("ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of()))
            .aggregations(new Aggregation("COUNT(*)", "COUNT"))
            .build();
    assertThat(
            ModelUtils.getTargetSql(
                build.getTarget(),
                null,
                null,
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                false,
                null))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`, COUNT(*) AS `COUNT` FROM PCOLLECTION GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`");
  }

  @Test
  public void should_generate_correct_sql_statement_for_transforms_with_aggregations() {
    final var nodeSpec =
        new TransformBuilder(
                specForNode(
                    "Person",
                    mapOf("ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of()))
            .aggregations(
                new Aggregation("COUNT(*)", "NUMBER_OF_PEOPLE"),
                new Aggregation("MAX(DATE_OF_BIRTH)", "YOUNGEST"))
            .build();
    assertThat(
            ModelUtils.getTargetSql(
                nodeSpec.getTarget(),
                null,
                null,
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                false,
                null))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`, COUNT(*) AS `NUMBER_OF_PEOPLE`, MAX(DATE_OF_BIRTH) AS `YOUNGEST` FROM PCOLLECTION GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`");
  }

  @Test
  public void should_generate_correct_sql_statement_for_transforms_with_aggregations_where_limit() {
    final var nodeSpec =
        new TransformBuilder(
                specForNode(
                    "Person",
                    mapOf("ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of()))
            .aggregations(
                new Aggregation("COUNT(*)", "NUMBER_OF_PEOPLE"),
                new Aggregation("MAX(DATE_OF_BIRTH)", "YOUNGEST"))
            .where("NAME LIKE 'A%'")
            .limit(1000)
            .build();
    assertThat(
            ModelUtils.getTargetSql(
                nodeSpec.getTarget(),
                null,
                null,
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                false,
                null))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`, COUNT(*) AS `NUMBER_OF_PEOPLE`, MAX(DATE_OF_BIRTH) AS `YOUNGEST` FROM PCOLLECTION WHERE NAME LIKE 'A%' GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH` LIMIT 1000");
  }

  @Test
  public void should_generate_correct_sql_statement_for_transforms_with_base_sql_sort_group() {
    final TargetQuerySpec relSpec =
        new TransformBuilder(
                specForRel(
                    "WORKS_AT",
                    mapOf("ID", "id", "CONTRACT_DATE", "contractDate"),
                    List.of("ID"),
                    "Person",
                    mapOf("PERSON_ID", "id"),
                    "Company",
                    mapOf("COMPANY_ID", "id")))
            .group(true)
            .build();
    assertThat(
            ModelUtils.getTargetSql(
                relSpec.getTarget(),
                relSpec.getStartNodeTarget(),
                relSpec.getEndNodeTarget(),
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                true,
                "SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE"))
        .isEqualTo(
            "SELECT `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID` FROM (SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE) GROUP BY `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID` ORDER BY `COMPANY_ID`");
  }

  @Test
  public void
      should_generate_correct_sql_statement_for_transforms_with_base_sql_sort_aggregations_limit() {
    var relSpec =
        new TransformBuilder(
                specForRel(
                    "WORKS_AT",
                    mapOf("ID", "id", "CONTRACT_DATE", "contractDate"),
                    List.of("ID"),
                    "Person",
                    mapOf("PERSON_ID", "id"),
                    "Company",
                    mapOf("COMPANY_ID", "id")))
            .aggregations(new Aggregation("COUNT(*)", "ENTRIES"))
            .limit(1000)
            .build();
    assertThat(
            ModelUtils.getTargetSql(
                relSpec.getTarget(),
                relSpec.getStartNodeTarget(),
                relSpec.getEndNodeTarget(),
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                true,
                "SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE"))
        .isEqualTo(
            "SELECT `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID`, COUNT(*) AS `ENTRIES` FROM (SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE) GROUP BY `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID` ORDER BY `COMPANY_ID` LIMIT 1000");
  }

  @Test
  public void
      should_generate_correct_sql_statement_for_transforms_with_base_sql_sort_aggregations_limit_where() {
    var relSpec =
        new TransformBuilder(
                specForRel(
                    "WORKS_AT",
                    mapOf("ID", "id", "CONTRACT_DATE", "contractDate"),
                    List.of("ID"),
                    "Person",
                    mapOf("PERSON_ID", "id"),
                    "Company",
                    mapOf("COMPANY_ID", "id")))
            .aggregations(new Aggregation("COUNT(*)", "ENTRIES"))
            .where("PERSON_ID BETWEEN 0 AND 10000")
            .limit(1000)
            .build();
    assertThat(
            ModelUtils.getTargetSql(
                relSpec.getTarget(),
                relSpec.getStartNodeTarget(),
                relSpec.getEndNodeTarget(),
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                true,
                "SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE"))
        .isEqualTo(
            "SELECT `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID`, COUNT(*) AS `ENTRIES` FROM (SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE) WHERE PERSON_ID BETWEEN 0 AND 10000 GROUP BY `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID` ORDER BY `COMPANY_ID` LIMIT 1000");
  }

  @SuppressWarnings("SameParameterValue")
  private static TargetQuerySpec specForNode(
      String label, Map<String, String> fieldToPropMapping, List<String> keyFields) {

    List<PropertyMapping> mappings = propertyMappings(fieldToPropMapping);
    NodeSchema schema =
        new NodeSchema(
            null, nodeKeyConstraints(label, keyFields), null, null, null, null, null, null, null);
    var target =
        new NodeTarget(
            true,
            label,
            "a-source",
            null,
            WriteMode.CREATE,
            null,
            List.of(label),
            mappings,
            schema);

    return new TargetQuerySpec.TargetQuerySpecBuilder()
        .sourceBeamSchema(Schema.builder().build())
        .target(target)
        .build();
  }

  @SuppressWarnings("SameParameterValue")
  private static TargetQuerySpec specForRel(
      String type,
      Map<String, String> fieldToPropMapping,
      List<String> keyFields,
      String sourceLabel,
      Map<String, String> sourceKeys,
      String targetLabel,
      Map<String, String> targetKeys) {

    var startNodeTarget =
        new NodeTarget(
            true,
            "start-node-target",
            "a-source",
            null,
            WriteMode.MERGE,
            null,
            List.of(sourceLabel),
            propertyMappings(sourceKeys),
            new NodeSchema(
                null,
                nodeKeyConstraints(targetLabel, sourceKeys.values()),
                null,
                null,
                null,
                null,
                null,
                null,
                null));
    var endNodeTarget =
        new NodeTarget(
            true,
            "end-node-target",
            "a-source",
            null,
            WriteMode.MERGE,
            null,
            List.of(targetLabel),
            propertyMappings(targetKeys),
            new NodeSchema(
                null,
                nodeKeyConstraints(targetLabel, targetKeys.values()),
                null,
                null,
                null,
                null,
                null,
                null,
                null));
    var target =
        new RelationshipTarget(
            true,
            type,
            "a-source",
            null,
            type,
            WriteMode.CREATE,
            NodeMatchMode.MERGE,
            null,
            "start-node-target",
            "end-node-target",
            propertyMappings(fieldToPropMapping),
            new RelationshipSchema(
                null, relKeyConstraints(keyFields), null, null, null, null, null, null, null));

    return new TargetQuerySpec.TargetQuerySpecBuilder()
        .sourceBeamSchema(Schema.builder().build())
        .target(target)
        .startNodeTarget(startNodeTarget)
        .endNodeTarget(endNodeTarget)
        .build();
  }

  private static List<PropertyMapping> propertyMappings(Map<String, String> fieldToPropMapping) {
    return fieldToPropMapping.entrySet().stream()
        .map(entry -> new PropertyMapping(entry.getKey(), entry.getValue(), null))
        .collect(Collectors.toList());
  }

  private static <K, V> Map<K, V> mapOf(K k1, V v1) {
    Map<K, V> result = new LinkedHashMap<>();
    result.put(k1, v1);
    return result;
  }

  private static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2) {
    Map<K, V> result = new LinkedHashMap<>();
    result.put(k1, v1);
    result.put(k2, v2);
    return result;
  }

  private static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
    Map<K, V> result = new LinkedHashMap<>();
    result.put(k1, v1);
    result.put(k2, v2);
    result.put(k3, v3);
    result.put(k4, v4);
    return result;
  }

  private static class TransformBuilder {

    private final TargetQuerySpecBuilder result;

    TransformBuilder(TargetQuerySpec original) {
      this.result =
          new TargetQuerySpec.TargetQuerySpecBuilder()
              .nullableSourceRows(original.getNullableSourceRows())
              .sourceBeamSchema(original.getSourceBeamSchema())
              .target(original.getTarget())
              .startNodeTarget(original.getStartNodeTarget())
              .endNodeTarget(original.getEndNodeTarget());
    }

    TransformBuilder aggregations(Aggregation... aggregations) {
      result.target(copyTarget(result.getTarget(), transformationsWithAggregations(aggregations)));
      return this;
    }

    TransformBuilder limit(int limit) {
      result.target(copyTarget(result.getTarget(), transformationsWithLimit(limit)));
      return this;
    }

    TransformBuilder where(String where) {
      result.target(copyTarget(result.getTarget(), transformationsWithWhere(where)));
      return this;
    }

    TransformBuilder group(boolean group) {
      result.target(copyTarget(result.getTarget(), transformationsWithGroupBy(group)));
      return this;
    }

    TargetQuerySpec build() {
      return result.build();
    }

    private static Target copyTarget(
        Target previousTarget,
        Function<SourceTransformations, SourceTransformations> copyTransform) {
      switch (previousTarget.getTargetType()) {
        case NODE:
          var originalNode = (NodeTarget) previousTarget;
          return copyNode(
              (NodeTarget) previousTarget,
              copyTransform.apply(originalNode.getSourceTransformations()));
        case RELATIONSHIP:
          var originalRelationship = (RelationshipTarget) previousTarget;
          return copyRelationship(
              originalRelationship,
              copyTransform.apply(originalRelationship.getSourceTransformations()));
        case QUERY:
        default:
          Assert.fail("expected node or relationship target");
          return null;
      }
    }

    private static NodeTarget copyNode(
        NodeTarget original, SourceTransformations newTransformations) {
      return new NodeTarget(
          original.isActive(),
          original.getName(),
          original.getSource(),
          original.getDependencies(),
          original.getWriteMode(),
          newTransformations,
          original.getLabels(),
          original.getProperties(),
          original.getSchema());
    }

    private static Function<SourceTransformations, SourceTransformations>
        transformationsWithAggregations(Aggregation[] aggregations) {
      return (original) ->
          new SourceTransformations(
              original != null && original.isEnableGrouping(),
              Arrays.asList(aggregations),
              original == null ? null : original.getWhereClause(),
              original == null ? null : original.getOrderByClauses(),
              original == null ? null : original.getLimit());
    }

    private Function<SourceTransformations, SourceTransformations> transformationsWithLimit(
        int limit) {
      return (original) ->
          new SourceTransformations(
              original != null && original.isEnableGrouping(),
              original == null ? null : original.getAggregations(),
              original == null ? null : original.getWhereClause(),
              original == null ? null : original.getOrderByClauses(),
              limit);
    }

    private Function<SourceTransformations, SourceTransformations> transformationsWithWhere(
        String where) {
      return (original) ->
          new SourceTransformations(
              original != null && original.isEnableGrouping(),
              original == null ? null : original.getAggregations(),
              where,
              original == null ? null : original.getOrderByClauses(),
              original == null ? null : original.getLimit());
    }

    private Function<SourceTransformations, SourceTransformations> transformationsWithGroupBy(
        boolean groupBy) {
      return (original) ->
          new SourceTransformations(
              groupBy,
              original == null ? null : original.getAggregations(),
              original == null ? null : original.getWhereClause(),
              original == null ? null : original.getOrderByClauses(),
              original == null ? null : original.getLimit());
    }

    private static RelationshipTarget copyRelationship(
        RelationshipTarget original, SourceTransformations transformations) {
      return new RelationshipTarget(
          original.isActive(),
          original.getName(),
          original.getSource(),
          original.getDependencies(),
          original.getType(),
          original.getWriteMode(),
          original.getNodeMatchMode(),
          transformations,
          original.getStartNodeReference(),
          original.getEndNodeReference(),
          original.getProperties(),
          original.getSchema());
    }
  }

  private static List<NodeKeyConstraint> nodeKeyConstraints(
      String label, Collection<String> keyFields) {
    return keyFields.stream()
        .map(
            prop ->
                new NodeKeyConstraint(String.format("key-%s", prop), label, List.of(prop), null))
        .collect(Collectors.toList());
  }

  private static List<RelationshipKeyConstraint> relKeyConstraints(Collection<String> keyFields) {
    return keyFields.stream()
        .map(key -> new RelationshipKeyConstraint(String.format("key-%s", key), List.of(key), null))
        .collect(Collectors.toList());
  }
}
