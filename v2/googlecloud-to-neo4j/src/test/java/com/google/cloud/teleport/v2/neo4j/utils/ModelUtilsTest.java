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

import static com.google.cloud.teleport.v2.neo4j.utils.SourceTransformationsBuilder.sourceTransformationsBuilder;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.neo4j.transforms.Aggregation;
import com.google.cloud.teleport.v2.neo4j.transforms.Order;
import com.google.cloud.teleport.v2.neo4j.transforms.OrderBy;
import com.google.cloud.teleport.v2.neo4j.transforms.SourceTransformations;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;
import org.neo4j.importer.v1.pipeline.NodeTargetStep;
import org.neo4j.importer.v1.pipeline.RelationshipTargetStep;
import org.neo4j.importer.v1.pipeline.TargetStep;
import org.neo4j.importer.v1.targets.NodeKeyConstraint;
import org.neo4j.importer.v1.targets.NodeMatchMode;
import org.neo4j.importer.v1.targets.NodeReference;
import org.neo4j.importer.v1.targets.NodeSchema;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.RelationshipKeyConstraint;
import org.neo4j.importer.v1.targets.RelationshipSchema;
import org.neo4j.importer.v1.targets.RelationshipTarget;
import org.neo4j.importer.v1.targets.WriteMode;

public class ModelUtilsTest {

  @Test
  public void should_generate_correct_sql_statement_for_nodes_no_keys() {
    assertThat(
            ModelUtils.getTargetSql(
                nodeTargetStep(
                    "Person",
                    Map.of(
                        "ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of()),
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                false,
                null))
        .isEqualTo("SELECT * FROM PCOLLECTION");
  }

  @Test
  public void should_generate_correct_sql_statement_for_nodes_with_keys() {
    assertThat(
            ModelUtils.getTargetSql(
                nodeTargetStep(
                    "Person",
                    Map.of(
                        "ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of("ID")),
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                false,
                null))
        .isEqualTo("SELECT * FROM PCOLLECTION");
  }

  @Test
  public void should_generate_correct_sql_statement_for_nodes_with_base_sql() {
    assertThat(
            ModelUtils.getTargetSql(
                nodeTargetStep(
                    "Person",
                    Map.of(
                        "ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of("ID")),
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
                nodeTargetStep(
                    "Person",
                    Map.of(
                        "ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of("ID")),
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                true,
                "SELECT ID, NAME, SURNAME, DATE_OF_BIRTH, BIRTH_PLACE FROM TABLE"))
        .isEqualTo(
            "SELECT * FROM (SELECT ID, NAME, SURNAME, DATE_OF_BIRTH, BIRTH_PLACE FROM TABLE)");
  }

  @Test
  public void should_generate_correct_sql_statement_for_relationships_no_keys() {
    assertThat(
            ModelUtils.getTargetSql(
                relationshipTargetStep(
                    "WORKS_AT",
                    Map.of("CONTRACT_DATE", "contractDate"),
                    List.of(),
                    "Person",
                    Map.of("PERSON_ID", "id"),
                    "Company",
                    Map.of("COMPANY_ID", "id")),
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                false,
                null))
        .isEqualTo("SELECT * FROM PCOLLECTION");
  }

  @Test
  public void should_generate_correct_sql_statement_for_relationships_with_keys() {
    assertThat(
            ModelUtils.getTargetSql(
                relationshipTargetStep(
                    "WORKS_AT",
                    Map.of("ID", "id", "CONTRACT_DATE", "contractDate"),
                    List.of("ID"),
                    "Person",
                    Map.of("PERSON_ID", "id"),
                    "Company",
                    Map.of("COMPANY_ID", "id")),
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                false,
                null))
        .isEqualTo("SELECT * FROM PCOLLECTION");
  }

  @Test
  public void should_generate_correct_sql_statement_for_relationships_with_base_sql() {
    assertThat(
            ModelUtils.getTargetSql(
                relationshipTargetStep(
                    "WORKS_AT",
                    Map.of("ID", "id", "CONTRACT_DATE", "contractDate"),
                    List.of("ID"),
                    "Person",
                    Map.of("PERSON_ID", "id"),
                    "Company",
                    Map.of("COMPANY_ID", "id")),
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                false,
                "SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE"))
        .isEqualTo("SELECT * FROM (SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE)");
  }

  @Test
  public void should_generate_correct_sql_statement_for_relationships_with_base_sql_sort() {
    assertThat(
            ModelUtils.getTargetSql(
                relationshipTargetStep(
                    "WORKS_AT",
                    Map.of("ID", "id", "CONTRACT_DATE", "contractDate"),
                    List.of("ID"),
                    "Person",
                    Map.of("PERSON_ID", "id"),
                    "Company",
                    Map.of("COMPANY_ID", "id")),
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                true,
                "SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE"))
        .isEqualTo(
            "SELECT * FROM (SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE) ORDER BY `COMPANY_ID`");
  }

  @Test
  public void should_generate_correct_sql_statement_for_transforms_with_grouping() {
    assertThat(
            ModelUtils.getTargetSql(
                nodeTargetStep(
                    "Person",
                    mapOf("ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of(),
                    sourceTransformationsBuilder().enableGrouping().build()),
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                false))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH` FROM PCOLLECTION GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`");
  }

  @Test
  public void should_generate_correct_sql_statement_for_transforms_with_grouping_limited() {
    assertThat(
            ModelUtils.getTargetSql(
                nodeTargetStep(
                    "Person",
                    mapOf("ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of(),
                    sourceTransformationsBuilder().enableGrouping().limit(100).build()),
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                false,
                null))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH` FROM PCOLLECTION GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH` LIMIT 100");
  }

  @Test
  public void should_generate_correct_sql_statement_for_transforms_with_grouping_where() {
    assertThat(
            ModelUtils.getTargetSql(
                nodeTargetStep(
                    "Person",
                    mapOf("ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of(),
                    sourceTransformationsBuilder()
                        .enableGrouping()
                        .where("NAME LIKE 'A%'")
                        .build()),
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                false,
                null))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH` FROM PCOLLECTION WHERE NAME LIKE 'A%' GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`");
  }

  @Test
  public void should_generate_correct_sql_statement_for_transforms_with_aggregation() {
    assertThat(
            ModelUtils.getTargetSql(
                nodeTargetStep(
                    "Person",
                    mapOf("ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of(),
                    sourceTransformationsBuilder().addAggregation("COUNT(*)", "COUNT").build()),
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                false,
                null))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`, COUNT(*) AS `COUNT` FROM PCOLLECTION GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`");
  }

  @Test
  public void should_generate_correct_sql_statement_for_transforms_with_aggregations() {
    assertThat(
            ModelUtils.getTargetSql(
                nodeTargetStep(
                    "Person",
                    mapOf("ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of(),
                    sourceTransformationsBuilder()
                        .addAggregation("COUNT(*)", "NUMBER_OF_PEOPLE")
                        .addAggregation("MAX(DATE_OF_BIRTH)", "YOUNGEST")
                        .build()),
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                false,
                null))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`, COUNT(*) AS `NUMBER_OF_PEOPLE`, MAX(DATE_OF_BIRTH) AS `YOUNGEST` FROM PCOLLECTION GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`");
  }

  @Test
  public void should_generate_correct_sql_statement_for_transforms_with_aggregations_where_limit() {
    assertThat(
            ModelUtils.getTargetSql(
                nodeTargetStep(
                    "Person",
                    mapOf("ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of(),
                    sourceTransformationsBuilder()
                        .addAggregation("COUNT(*)", "NUMBER_OF_PEOPLE")
                        .addAggregation("MAX(DATE_OF_BIRTH)", "YOUNGEST")
                        .where("NAME LIKE 'A%'")
                        .limit(1000)
                        .build()),
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                false,
                null))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`, COUNT(*) AS `NUMBER_OF_PEOPLE`, MAX(DATE_OF_BIRTH) AS `YOUNGEST` FROM PCOLLECTION WHERE NAME LIKE 'A%' GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH` LIMIT 1000");
  }

  @Test
  public void should_generate_correct_sql_statement_for_transforms_with_base_sql_sort_group() {
    assertThat(
            ModelUtils.getTargetSql(
                relationshipTargetStep(
                    "WORKS_AT",
                    mapOf("ID", "id", "CONTRACT_DATE", "contractDate"),
                    List.of("id"),
                    "Person",
                    mapOf("PERSON_ID", "id"),
                    "Company",
                    mapOf("COMPANY_ID", "id"),
                    sourceTransformationsBuilder().enableGrouping().build()),
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                true,
                "SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE"))
        .isEqualTo(
            "SELECT `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID` FROM (SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE) GROUP BY `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID` ORDER BY `COMPANY_ID`");
  }

  @Test
  public void
      should_generate_correct_sql_statement_for_transforms_with_base_sql_sort_aggregations_limit() {
    assertThat(
            ModelUtils.getTargetSql(
                relationshipTargetStep(
                    "WORKS_AT",
                    mapOf("ID", "id", "CONTRACT_DATE", "contractDate"),
                    List.of("id"),
                    "Person",
                    mapOf("PERSON_ID", "id"),
                    "Company",
                    mapOf("COMPANY_ID", "id"),
                    sourceTransformationsBuilder()
                        .addAggregation("COUNT(*)", "ENTRIES")
                        .limit(1000)
                        .build()),
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                true,
                "SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE"))
        .isEqualTo(
            "SELECT `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID`, COUNT(*) AS `ENTRIES` FROM (SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE) GROUP BY `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID` ORDER BY `COMPANY_ID` LIMIT 1000");
  }

  @Test
  public void
      should_generate_correct_sql_statement_for_transforms_with_base_sql_sort_aggregations_limit_where() {
    assertThat(
            ModelUtils.getTargetSql(
                relationshipTargetStep(
                    "WORKS_AT",
                    mapOf("ID", "id", "CONTRACT_DATE", "contractDate"),
                    List.of("id"),
                    "Person",
                    mapOf("PERSON_ID", "id"),
                    "Company",
                    mapOf("COMPANY_ID", "id"),
                    sourceTransformationsBuilder()
                        .addAggregation("COUNT(*)", "ENTRIES")
                        .where("PERSON_ID BETWEEN 0 AND 10000")
                        .limit(1000)
                        .build()),
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                true,
                "SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE"))
        .isEqualTo(
            "SELECT `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID`, COUNT(*) AS `ENTRIES` FROM (SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE) WHERE PERSON_ID BETWEEN 0 AND 10000 GROUP BY `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID` ORDER BY `COMPANY_ID` LIMIT 1000");
  }

  @SuppressWarnings("SameParameterValue")
  private static TargetStep nodeTargetStep(
      String label, Map<String, String> fieldToPropMapping, List<String> keyFields) {

    return nodeTargetStep(label, fieldToPropMapping, keyFields, null);
  }

  @SuppressWarnings("SameParameterValue")
  private static TargetStep nodeTargetStep(
      String label,
      Map<String, String> fieldToPropMapping,
      List<String> keyFields,
      SourceTransformations sourceTransformations) {

    List<PropertyMapping> mappings = propertyMappings(fieldToPropMapping);
    NodeSchema schema =
        new NodeSchema(
            null, nodeKeyConstraints(label, keyFields), null, null, null, null, null, null, null);
    return new NodeTargetStep(
        new NodeTarget(
            true,
            label,
            "a-source",
            null,
            WriteMode.CREATE,
            sourceTransformations == null ? List.of() : List.of(sourceTransformations),
            List.of(label),
            mappings,
            schema),
        Set.of());
  }

  @SuppressWarnings("SameParameterValue")
  private static TargetStep relationshipTargetStep(
      String type,
      Map<String, String> fieldToPropMapping,
      List<String> keyFields,
      String sourceLabel,
      Map<String, String> sourceKeys,
      String targetLabel,
      Map<String, String> targetKeys) {
    return relationshipTargetStep(
        type,
        fieldToPropMapping,
        keyFields,
        sourceLabel,
        sourceKeys,
        targetLabel,
        targetKeys,
        null);
  }

  @SuppressWarnings("SameParameterValue")
  private static TargetStep relationshipTargetStep(
      String type,
      Map<String, String> fieldToPropMapping,
      List<String> keyProperties,
      String sourceLabel,
      Map<String, String> sourceKeys,
      String targetLabel,
      Map<String, String> targetKeys,
      SourceTransformations relationshipSourceTransformations) {

    var startNodeTarget =
        new NodeTarget(
            true,
            "start-node-target",
            "a-source",
            null,
            WriteMode.MERGE,
            List.of(),
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
            List.of(),
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
            relationshipSourceTransformations == null
                ? List.of()
                : List.of(relationshipSourceTransformations),
            new NodeReference("start-node-target"),
            new NodeReference("end-node-target"),
            propertyMappings(fieldToPropMapping),
            new RelationshipSchema(
                null, relKeyConstraints(keyProperties), null, null, null, null, null, null, null));

    return new RelationshipTargetStep(
        target,
        new NodeTargetStep(startNodeTarget, Set.of()),
        new NodeTargetStep(endNodeTarget, Set.of()),
        Set.of());
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

class SourceTransformationsBuilder {

  private boolean enableGrouping;

  private List<Aggregation> aggregations = new ArrayList<>();

  private String where;

  private List<OrderBy> orderBys = new ArrayList<>();

  private Integer limit = -1;

  public static SourceTransformationsBuilder sourceTransformationsBuilder() {
    return new SourceTransformationsBuilder();
  }

  public static SourceTransformationsBuilder sourceTransformationsBuilder(
      SourceTransformationsBuilder builder) {
    return new SourceTransformationsBuilder(builder);
  }

  private SourceTransformationsBuilder() {}

  private SourceTransformationsBuilder(SourceTransformationsBuilder builder) {
    this.enableGrouping = builder.enableGrouping;
    this.aggregations = builder.aggregations;
    this.where = builder.where;
    this.orderBys = builder.orderBys;
    this.limit = builder.limit;
  }

  public SourceTransformationsBuilder enableGrouping() {
    this.enableGrouping = true;
    return this;
  }

  public SourceTransformationsBuilder addAggregation(String expression, String fieldName) {
    this.aggregations.add(new Aggregation(expression, fieldName));
    return this;
  }

  public SourceTransformationsBuilder where(String whereClause) {
    this.where = whereClause;
    return this;
  }

  public SourceTransformationsBuilder addOrderByAsc(String expression) {
    this.orderBys.add(new OrderBy(expression, Order.ASC));
    return this;
  }

  public SourceTransformationsBuilder addOrderByDesc(String expression) {
    this.orderBys.add(new OrderBy(expression, Order.DESC));
    return this;
  }

  public SourceTransformationsBuilder limit(Integer limit) {
    this.limit = limit;
    return this;
  }

  public SourceTransformations build() {
    return new SourceTransformations(enableGrouping, aggregations, where, orderBys, limit);
  }
}
