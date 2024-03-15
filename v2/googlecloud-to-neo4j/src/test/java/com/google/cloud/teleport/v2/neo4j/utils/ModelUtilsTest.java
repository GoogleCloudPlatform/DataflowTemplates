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

import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.job.Aggregation;
import com.google.cloud.teleport.v2.neo4j.model.job.FieldNameTuple;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Test;

public class ModelUtilsTest {

  @Test
  public void shouldGenerateCorrectSqlStatementForNodes_NoKeys() {
    assertThat(
            ModelUtils.getTargetSql(
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                specForNode(
                    "Person",
                    Map.of(
                        "ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of()),
                false,
                null))
        .isEqualTo("SELECT * FROM PCOLLECTION");
  }

  @Test
  public void shouldGenerateCorrectSqlStatementForNodes_WithKeys() {
    assertThat(
            ModelUtils.getTargetSql(
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                specForNode(
                    "Person",
                    Map.of(
                        "ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of("ID")),
                false,
                null))
        .isEqualTo("SELECT * FROM PCOLLECTION");
  }

  @Test
  public void shouldGenerateCorrectSqlStatementForNodes_WithBaseSQL() {
    assertThat(
            ModelUtils.getTargetSql(
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                specForNode(
                    "Person",
                    Map.of(
                        "ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of("ID")),
                false,
                "SELECT ID, NAME, SURNAME, DATE_OF_BIRTH, BIRTH_PLACE FROM TABLE"))
        .isEqualTo(
            "SELECT * FROM (SELECT ID, NAME, SURNAME, DATE_OF_BIRTH, BIRTH_PLACE FROM TABLE)");
  }

  @Test
  public void shouldGenerateCorrectSqlStatementForNodes_WithBaseSQL_Sort() {
    assertThat(
            ModelUtils.getTargetSql(
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                specForNode(
                    "Person",
                    Map.of(
                        "ID", "id", "NAME", "name", "SURNAME", "surname", "DATE_OF_BIRTH", "dob"),
                    List.of("ID")),
                true,
                "SELECT ID, NAME, SURNAME, DATE_OF_BIRTH, BIRTH_PLACE FROM TABLE"))
        .isEqualTo(
            "SELECT * FROM (SELECT ID, NAME, SURNAME, DATE_OF_BIRTH, BIRTH_PLACE FROM TABLE)");
  }

  @Test
  public void shouldGenerateCorrectSqlStatementForRelationships_NoKeys() {
    assertThat(
            ModelUtils.getTargetSql(
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                specForRel(
                    "WORKS_AT",
                    Map.of("CONTRACT_DATE", "contractDate"),
                    List.of(),
                    "Person",
                    Map.of("PERSON_ID", "id"),
                    "Company",
                    Map.of("COMPANY_ID", "id")),
                false,
                null))
        .isEqualTo("SELECT * FROM PCOLLECTION");
  }

  @Test
  public void shouldGenerateCorrectSqlStatementForRelationships_WithKeys() {
    assertThat(
            ModelUtils.getTargetSql(
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                specForRel(
                    "WORKS_AT",
                    Map.of("ID", "id", "CONTRACT_DATE", "contractDate"),
                    List.of("ID"),
                    "Person",
                    Map.of("PERSON_ID", "id"),
                    "Company",
                    Map.of("COMPANY_ID", "id")),
                false,
                null))
        .isEqualTo("SELECT * FROM PCOLLECTION");
  }

  @Test
  public void shouldGenerateCorrectSqlStatementForRelationships_WithBaseSQL() {
    assertThat(
            ModelUtils.getTargetSql(
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                specForRel(
                    "WORKS_AT",
                    Map.of("ID", "id", "CONTRACT_DATE", "contractDate"),
                    List.of("ID"),
                    "Person",
                    Map.of("PERSON_ID", "id"),
                    "Company",
                    Map.of("COMPANY_ID", "id")),
                false,
                "SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE"))
        .isEqualTo("SELECT * FROM (SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE)");
  }

  @Test
  public void shouldGenerateCorrectSqlStatementForRelationships_WithBaseSQL_Sort() {
    assertThat(
            ModelUtils.getTargetSql(
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                specForRel(
                    "WORKS_AT",
                    Map.of("ID", "id", "CONTRACT_DATE", "contractDate"),
                    List.of("ID"),
                    "Person",
                    Map.of("PERSON_ID", "id"),
                    "Company",
                    Map.of("COMPANY_ID", "id")),
                true,
                "SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE"))
        .isEqualTo(
            "SELECT * FROM (SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE) ORDER BY `COMPANY_ID`");
  }

  @Test
  public void shouldGenerateCorrectSqlStatementForTransforms_WithGrouping() {
    assertThat(
            ModelUtils.getTargetSql(
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                new TransformBuilder(
                        specForNode(
                            "Person",
                            mapOf(
                                "ID",
                                "id",
                                "NAME",
                                "name",
                                "SURNAME",
                                "surname",
                                "DATE_OF_BIRTH",
                                "dob"),
                            List.of()))
                    .group(true)
                    .build(),
                false,
                null))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH` FROM PCOLLECTION GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`");
  }

  @Test
  public void shouldGenerateCorrectSqlStatementForTransforms_WithGrouping_Limited() {
    assertThat(
            ModelUtils.getTargetSql(
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                new TransformBuilder(
                        specForNode(
                            "Person",
                            mapOf(
                                "ID",
                                "id",
                                "NAME",
                                "name",
                                "SURNAME",
                                "surname",
                                "DATE_OF_BIRTH",
                                "dob"),
                            List.of()))
                    .group(true)
                    .limit(100)
                    .build(),
                false,
                null))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH` FROM PCOLLECTION GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH` LIMIT 100");
  }

  @Test
  public void shouldGenerateCorrectSqlStatementForTransforms_WithGrouping_Where() {
    assertThat(
            ModelUtils.getTargetSql(
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                new TransformBuilder(
                        specForNode(
                            "Person",
                            mapOf(
                                "ID",
                                "id",
                                "NAME",
                                "name",
                                "SURNAME",
                                "surname",
                                "DATE_OF_BIRTH",
                                "dob"),
                            List.of()))
                    .group(true)
                    .where("NAME LIKE 'A%'")
                    .build(),
                false,
                null))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH` FROM PCOLLECTION WHERE NAME LIKE 'A%' GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`");
  }

  @Test
  public void shouldGenerateCorrectSqlStatementForTransforms_WithAggregation() {
    assertThat(
            ModelUtils.getTargetSql(
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                new TransformBuilder(
                        specForNode(
                            "Person",
                            mapOf(
                                "ID",
                                "id",
                                "NAME",
                                "name",
                                "SURNAME",
                                "surname",
                                "DATE_OF_BIRTH",
                                "dob"),
                            List.of()))
                    .aggregations(aggregation("COUNT", "COUNT(*)"))
                    .build(),
                false,
                null))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`, COUNT(*) AS `COUNT` FROM PCOLLECTION GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`");
  }

  @Test
  public void shouldGenerateCorrectSqlStatementForTransforms_WithAggregations() {
    assertThat(
            ModelUtils.getTargetSql(
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                new TransformBuilder(
                        specForNode(
                            "Person",
                            mapOf(
                                "ID",
                                "id",
                                "NAME",
                                "name",
                                "SURNAME",
                                "surname",
                                "DATE_OF_BIRTH",
                                "dob"),
                            List.of()))
                    .aggregations(
                        aggregation("NUMBER_OF_PEOPLE", "COUNT(*)"),
                        aggregation("YOUNGEST", "MAX(DATE_OF_BIRTH)"))
                    .build(),
                false,
                null))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`, COUNT(*) AS `NUMBER_OF_PEOPLE`, MAX(DATE_OF_BIRTH) AS `YOUNGEST` FROM PCOLLECTION GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`");
  }

  @Test
  public void shouldGenerateCorrectSqlStatementForTransforms_WithAggregations_Where_Limit() {
    assertThat(
            ModelUtils.getTargetSql(
                Set.of("ID", "NAME", "SURNAME", "DATE_OF_BIRTH"),
                new TransformBuilder(
                        specForNode(
                            "Person",
                            mapOf(
                                "ID",
                                "id",
                                "NAME",
                                "name",
                                "SURNAME",
                                "surname",
                                "DATE_OF_BIRTH",
                                "dob"),
                            List.of()))
                    .aggregations(
                        aggregation("NUMBER_OF_PEOPLE", "COUNT(*)"),
                        aggregation("YOUNGEST", "MAX(DATE_OF_BIRTH)"))
                    .where("NAME LIKE 'A%'")
                    .limit(1000)
                    .build(),
                false,
                null))
        .isEqualTo(
            "SELECT `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH`, COUNT(*) AS `NUMBER_OF_PEOPLE`, MAX(DATE_OF_BIRTH) AS `YOUNGEST` FROM PCOLLECTION WHERE NAME LIKE 'A%' GROUP BY `ID`, `NAME`, `SURNAME`, `DATE_OF_BIRTH` LIMIT 1000");
  }

  @Test
  public void shouldGenerateCorrectSqlStatementForTransforms_WithBaseSQL_Sort_Group() {
    assertThat(
            ModelUtils.getTargetSql(
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
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
                    .build(),
                true,
                "SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE"))
        .isEqualTo(
            "SELECT `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID` FROM (SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE) GROUP BY `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID` ORDER BY `COMPANY_ID`");
  }

  @Test
  public void shouldGenerateCorrectSqlStatementForTransforms_WithBaseSQL_Sort_Aggregations_Limit() {
    assertThat(
            ModelUtils.getTargetSql(
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                new TransformBuilder(
                        specForRel(
                            "WORKS_AT",
                            mapOf("ID", "id", "CONTRACT_DATE", "contractDate"),
                            List.of("ID"),
                            "Person",
                            mapOf("PERSON_ID", "id"),
                            "Company",
                            mapOf("COMPANY_ID", "id")))
                    .aggregations(aggregation("ENTRIES", "COUNT(*)"))
                    .limit(1000)
                    .build(),
                true,
                "SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE"))
        .isEqualTo(
            "SELECT `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID`, COUNT(*) AS `ENTRIES` FROM (SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE) GROUP BY `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID` ORDER BY `COMPANY_ID` LIMIT 1000");
  }

  @Test
  public void
      shouldGenerateCorrectSqlStatementForTransforms_WithBaseSQL_Sort_Aggregations_Limit_Where() {
    assertThat(
            ModelUtils.getTargetSql(
                Set.of("ID", "PERSON_ID", "COMPANY_ID", "CONTRACT_DATE"),
                new TransformBuilder(
                        specForRel(
                            "WORKS_AT",
                            mapOf("ID", "id", "CONTRACT_DATE", "contractDate"),
                            List.of("ID"),
                            "Person",
                            mapOf("PERSON_ID", "id"),
                            "Company",
                            mapOf("COMPANY_ID", "id")))
                    .aggregations(aggregation("ENTRIES", "COUNT(*)"))
                    .where("PERSON_ID BETWEEN 0 AND 10000")
                    .limit(1000)
                    .build(),
                true,
                "SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE"))
        .isEqualTo(
            "SELECT `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID`, COUNT(*) AS `ENTRIES` FROM (SELECT ID, PERSON_ID, COMPANY_ID, CONTRACT_DATE FROM TABLE) WHERE PERSON_ID BETWEEN 0 AND 10000 GROUP BY `ID`, `CONTRACT_DATE`, `PERSON_ID`, `COMPANY_ID` ORDER BY `COMPANY_ID` LIMIT 1000");
  }

  @SuppressWarnings("SameParameterValue")
  private static TargetQuerySpec specForNode(
      String label, Map<String, String> fieldToPropMapping, List<String> keyFields) {
    Set<String> sourceFields = fieldToPropMapping.keySet();

    Target target = new Target();
    target.setName(label);
    target.setType(TargetType.node);
    target.getMappings().add(new Mapping(FragmentType.node, RoleType.label, fieldNameTuple(label)));
    target
        .getMappings()
        .addAll(
            fieldToPropMapping.entrySet().stream()
                .map(
                    e ->
                        new Mapping(
                            FragmentType.node,
                            RoleType.property,
                            fieldNameTuple(e.getKey(), e.getValue())))
                .collect(Collectors.toList()));
    target
        .getMappings()
        .addAll(
            keyFields.stream()
                .map(k -> Map.entry(k, fieldToPropMapping.get(k)))
                .map(
                    e ->
                        new Mapping(
                            FragmentType.node,
                            RoleType.key,
                            fieldNameTuple(e.getKey(), e.getValue())))
                .collect(Collectors.toList()));

    Source source = new Source();
    source.setFieldNames(sourceFields.toArray(String[]::new));

    return new TargetQuerySpec(source, Schema.builder().build(), null, target);
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
    Set<String> sourceFields = fieldToPropMapping.keySet();

    Target target = new Target();
    target.setName(type);
    target.setType(TargetType.edge);
    target.getMappings().add(new Mapping(FragmentType.rel, RoleType.type, fieldNameTuple(type)));
    target
        .getMappings()
        .addAll(
            fieldToPropMapping.entrySet().stream()
                .map(
                    e ->
                        new Mapping(
                            FragmentType.rel,
                            RoleType.property,
                            fieldNameTuple(e.getKey(), e.getValue())))
                .collect(Collectors.toList()));
    target
        .getMappings()
        .addAll(
            keyFields.stream()
                .map(k -> Map.entry(k, fieldToPropMapping.get(k)))
                .map(
                    e ->
                        new Mapping(
                            FragmentType.rel,
                            RoleType.key,
                            fieldNameTuple(e.getKey(), e.getValue())))
                .collect(Collectors.toList()));
    target
        .getMappings()
        .add(new Mapping(FragmentType.source, RoleType.label, fieldNameTuple(sourceLabel)));
    target
        .getMappings()
        .addAll(
            sourceKeys.entrySet().stream()
                .map(
                    e ->
                        new Mapping(
                            FragmentType.source,
                            RoleType.key,
                            fieldNameTuple(e.getKey(), e.getValue())))
                .collect(Collectors.toList()));
    target
        .getMappings()
        .add(new Mapping(FragmentType.target, RoleType.label, fieldNameTuple(targetLabel)));
    target
        .getMappings()
        .addAll(
            targetKeys.entrySet().stream()
                .map(
                    e ->
                        new Mapping(
                            FragmentType.target,
                            RoleType.key,
                            fieldNameTuple(e.getKey(), e.getValue())))
                .collect(Collectors.toList()));

    Source source = new Source();
    source.setFieldNames(sourceFields.toArray(String[]::new));

    return new TargetQuerySpec(source, Schema.builder().build(), null, target);
  }

  private static FieldNameTuple fieldNameTuple(String field, String property) {
    FieldNameTuple result = new FieldNameTuple();
    result.setName(property);
    result.setField(field);
    return result;
  }

  private static FieldNameTuple fieldNameTuple(String constant) {
    FieldNameTuple result = new FieldNameTuple();
    result.setConstant(constant);
    return result;
  }

  private static Aggregation aggregation(String field, String expression) {
    Aggregation result = new Aggregation();
    result.setField(field);
    result.setExpression(expression);
    return result;
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

  private static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3) {
    Map<K, V> result = new LinkedHashMap<>();
    result.put(k1, v1);
    result.put(k2, v2);
    result.put(k3, v3);
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

    private final TargetQuerySpec original;

    TransformBuilder(TargetQuerySpec original) {
      this.original = original;
    }

    TransformBuilder aggregations(Aggregation... aggregations) {
      original.getTarget().getTransform().setAggregations(Arrays.asList(aggregations));
      return this;
    }

    TransformBuilder limit(int limit) {
      original.getTarget().getTransform().setLimit(limit);
      return this;
    }

    TransformBuilder where(String where) {
      original.getTarget().getTransform().setWhere(where);
      return this;
    }

    TransformBuilder orderBy(String orderBy) {
      original.getTarget().getTransform().setOrderBy(orderBy);
      return this;
    }

    TransformBuilder group(boolean group) {
      original.getTarget().getTransform().setGroup(group);
      return this;
    }

    TargetQuerySpec build() {
      return original;
    }
  }
}
