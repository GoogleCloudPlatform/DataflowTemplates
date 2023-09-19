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
package com.google.cloud.teleport.v2.neo4j.model;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.PropertyType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.Config;
import com.google.cloud.teleport.v2.neo4j.model.job.FieldNameTuple;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

public class InputRefactoringTest {

  private final JobSpec jobSpec = new JobSpec();
  private final Target target = new Target();
  private InputRefactoring refactorer;

  @Before
  public void prepare() {
    jobSpec.getTargets().add(target);
    refactorer = new InputRefactoring(new OptionsParams());
  }

  @Test
  public void interpolatesCustomTargetQueries() {
    OptionsParams options = new OptionsParams();
    options.overlayTokens("{\"foo\": \"bar\"}");
    target.setType(TargetType.custom_query);
    target.setCustomQuery("RETURN \"$foo\"");
    jobSpec.getTargets().add(target);
    InputRefactoring refactorer = new InputRefactoring(options);

    refactorer.refactorJobSpec(jobSpec);

    assertThat(target.getCustomQuery()).isEqualTo("RETURN \"bar\"");
  }

  @Test
  public void removesNodeTargetKeyMappingsFieldsFromUniqueMappings() {
    target.setType(TargetType.node);
    target.setName("key implies unique");
    addMapping(target, mapping(FragmentType.node, RoleType.key, "source_field", "targetProperty"));
    addMapping(
        target,
        uniqueMapping(FragmentType.node, RoleType.property, "source_field", "targetProperty"));
    assertThat(target.getMappings()).hasSize(2);

    refactorer.refactorJobSpec(jobSpec);

    List<Mapping> actualMappings =
        jobSpec.getTargets().stream()
            .flatMap(t -> t.getMappings().stream())
            .collect(Collectors.toList());
    assertThat(actualMappings)
        .isEqualTo(
            List.of(mapping(FragmentType.node, RoleType.key, "source_field", "targetProperty")));
  }

  @Test
  public void removesEdgeTargetKeyMappingsFieldsFromUniqueMappings() {
    target.setType(TargetType.edge);
    target.setName("key implies unique");
    addMapping(target, mapping(FragmentType.rel, RoleType.key, "source_field", "targetProperty"));
    addMapping(
        target,
        uniqueMapping(FragmentType.rel, RoleType.property, "source_field", "targetProperty"));
    assertThat(target.getMappings()).hasSize(2);

    refactorer.refactorJobSpec(jobSpec);

    List<Mapping> actualMappings =
        jobSpec.getTargets().stream()
            .flatMap(t -> t.getMappings().stream())
            .collect(Collectors.toList());
    assertThat(actualMappings)
        .isEqualTo(
            List.of(mapping(FragmentType.rel, RoleType.key, "source_field", "targetProperty")));
  }

  @Test
  public void removesNodeTargetKeyMappingsFromMandatoryMappings() {
    target.setType(TargetType.node);
    target.setName("key implies mandatory (non-null)");
    addMapping(target, mapping(FragmentType.node, RoleType.key, "source_field", "targetProperty"));
    addMapping(
        target,
        mandatoryMapping(FragmentType.node, RoleType.property, "source_field", "targetProperty"));
    assertThat(target.getMappings()).hasSize(2);

    refactorer.refactorJobSpec(jobSpec);

    List<Mapping> actualMappings =
        jobSpec.getTargets().stream()
            .flatMap(t -> t.getMappings().stream())
            .collect(Collectors.toList());
    assertThat(actualMappings)
        .isEqualTo(
            List.of(mapping(FragmentType.node, RoleType.key, "source_field", "targetProperty")));
  }

  @Test
  public void removesEdgeTargetKeyMappingsFromMandatoryMappings() {
    target.setType(TargetType.edge);
    target.setName("key implies mandatory (non-null)");
    addMapping(target, mapping(FragmentType.rel, RoleType.key, "source_field", "targetProperty"));
    addMapping(
        target,
        mandatoryMapping(FragmentType.rel, RoleType.property, "source_field", "targetProperty"));
    assertThat(target.getMappings()).hasSize(2);

    refactorer.refactorJobSpec(jobSpec);

    List<Mapping> actualMappings =
        jobSpec.getTargets().stream()
            .flatMap(t -> t.getMappings().stream())
            .collect(Collectors.toList());
    assertThat(actualMappings)
        .isEqualTo(
            List.of(mapping(FragmentType.rel, RoleType.key, "source_field", "targetProperty")));
  }

  @Test
  public void removesNodeTargetKeyMappingsFromIndexedMappings() {
    target.setType(TargetType.node);
    target.setName("key is always indexed");
    addMapping(target, mapping(FragmentType.node, RoleType.key, "source_field", "targetProperty"));
    addMapping(
        target,
        indexedMapping(FragmentType.node, RoleType.property, "source_field", "targetProperty"));
    assertThat(target.getMappings()).hasSize(2);

    refactorer.refactorJobSpec(jobSpec);

    List<Mapping> actualMappings =
        jobSpec.getTargets().stream()
            .flatMap(t -> t.getMappings().stream())
            .collect(Collectors.toList());
    assertThat(actualMappings)
        .isEqualTo(
            List.of(mapping(FragmentType.node, RoleType.key, "source_field", "targetProperty")));
  }

  @Test
  public void removesEdgeTargetKeyMappingsFromIndexedMappings() {
    target.setType(TargetType.edge);
    target.setName("key is always indexed");
    addMapping(target, mapping(FragmentType.rel, RoleType.key, "source_field", "targetProperty"));
    addMapping(
        target,
        indexedMapping(FragmentType.rel, RoleType.property, "source_field", "targetProperty"));
    assertThat(target.getMappings()).hasSize(2);

    refactorer.refactorJobSpec(jobSpec);

    List<Mapping> actualMappings =
        jobSpec.getTargets().stream()
            .flatMap(t -> t.getMappings().stream())
            .collect(Collectors.toList());
    assertThat(actualMappings)
        .isEqualTo(
            List.of(mapping(FragmentType.rel, RoleType.key, "source_field", "targetProperty")));
  }

  @Test
  public void removesNodeTargetUniqueMappingsFromIndexMappings() {
    target.setType(TargetType.node);
    target.setName("unique is always indexed");
    addMapping(
        target,
        uniqueMapping(FragmentType.node, RoleType.property, "source_field", "targetProperty"));
    addMapping(
        target,
        indexedMapping(FragmentType.node, RoleType.property, "source_field", "targetProperty"));
    assertThat(target.getMappings()).hasSize(2);

    refactorer.refactorJobSpec(jobSpec);

    List<Mapping> actualMappings =
        jobSpec.getTargets().stream()
            .flatMap(t -> t.getMappings().stream())
            .collect(Collectors.toList());
    assertThat(actualMappings)
        .isEqualTo(
            List.of(
                uniqueMapping(
                    FragmentType.node, RoleType.property, "source_field", "targetProperty")));
  }

  @Test
  public void removesEdgeTargetUniqueMappingsFromIndexMappings() {
    target.setType(TargetType.edge);
    target.setName("unique is always indexed");
    addMapping(
        target,
        uniqueMapping(FragmentType.rel, RoleType.property, "source_field", "targetProperty"));
    addMapping(
        target,
        indexedMapping(FragmentType.rel, RoleType.property, "source_field", "targetProperty"));
    assertThat(target.getMappings()).hasSize(2);

    refactorer.refactorJobSpec(jobSpec);

    List<Mapping> actualMappings =
        jobSpec.getTargets().stream()
            .flatMap(t -> t.getMappings().stream())
            .collect(Collectors.toList());
    assertThat(actualMappings)
        .isEqualTo(
            List.of(
                uniqueMapping(
                    FragmentType.rel, RoleType.property, "source_field", "targetProperty")));
  }

  @Test
  public void doesNotRemoveEdgeSourceKeyMappingsFromEdgeUniqueMappings() {
    target.setType(TargetType.edge);
    target.setName("edge source mappings do not overlap with rel unique mappings");
    addMapping(
        target, mapping(FragmentType.source, RoleType.key, "source_field", "targetProperty"));
    addMapping(
        target,
        uniqueMapping(FragmentType.rel, RoleType.property, "source_field", "targetProperty"));
    assertThat(target.getMappings()).hasSize(2);

    refactorer.refactorJobSpec(jobSpec);

    List<Mapping> actualMappings =
        jobSpec.getTargets().stream()
            .flatMap(t -> t.getMappings().stream())
            .collect(Collectors.toList());
    assertThat(actualMappings)
        .isEqualTo(
            List.of(
                mapping(FragmentType.source, RoleType.key, "source_field", "targetProperty"),
                uniqueMapping(
                    FragmentType.rel, RoleType.property, "source_field", "targetProperty")));
  }

  @Test
  public void doesNotRemoveEdgeSourceKeyMappingsFromEdgeMandatoryMappings() {
    target.setType(TargetType.edge);
    target.setName("edge source mappings do not overlap with rel mandatory (non-null) mappings");
    addMapping(
        target, mapping(FragmentType.source, RoleType.key, "source_field", "targetProperty"));
    addMapping(
        target,
        mandatoryMapping(FragmentType.rel, RoleType.property, "source_field", "targetProperty"));
    assertThat(target.getMappings()).hasSize(2);

    refactorer.refactorJobSpec(jobSpec);

    List<Mapping> actualMappings =
        jobSpec.getTargets().stream()
            .flatMap(t -> t.getMappings().stream())
            .collect(Collectors.toList());
    assertThat(actualMappings)
        .isEqualTo(
            List.of(
                mapping(FragmentType.source, RoleType.key, "source_field", "targetProperty"),
                mandatoryMapping(
                    FragmentType.rel, RoleType.property, "source_field", "targetProperty")));
  }

  @Test
  public void doesNotRemoveEdgeSourceKeyMappingsFromEdgeIndexedMappings() {
    target.setType(TargetType.edge);
    target.setName("edge source mappings do not overlap with rel mandatory (non-null) mappings");
    addMapping(
        target, mapping(FragmentType.source, RoleType.key, "source_field", "targetProperty"));
    addMapping(
        target,
        indexedMapping(FragmentType.rel, RoleType.property, "source_field", "targetProperty"));
    assertThat(target.getMappings()).hasSize(2);

    refactorer.refactorJobSpec(jobSpec);

    List<Mapping> actualMappings =
        jobSpec.getTargets().stream()
            .flatMap(t -> t.getMappings().stream())
            .collect(Collectors.toList());
    assertThat(actualMappings)
        .isEqualTo(
            List.of(
                mapping(FragmentType.source, RoleType.key, "source_field", "targetProperty"),
                indexedMapping(
                    FragmentType.rel, RoleType.property, "source_field", "targetProperty")));
  }

  @Test
  public void doesNotRemoveEdgeTargetKeyMappingsFromEdgeUniqueMappings() {
    target.setType(TargetType.edge);
    target.setName("edge target mappings do not overlap with rel unique mappings");
    addMapping(
        target, mapping(FragmentType.target, RoleType.key, "source_field", "targetProperty"));
    addMapping(
        target,
        uniqueMapping(FragmentType.rel, RoleType.property, "source_field", "targetProperty"));
    assertThat(target.getMappings()).hasSize(2);

    refactorer.refactorJobSpec(jobSpec);

    List<Mapping> actualMappings =
        jobSpec.getTargets().stream()
            .flatMap(t -> t.getMappings().stream())
            .collect(Collectors.toList());
    assertThat(actualMappings)
        .isEqualTo(
            List.of(
                mapping(FragmentType.target, RoleType.key, "source_field", "targetProperty"),
                uniqueMapping(
                    FragmentType.rel, RoleType.property, "source_field", "targetProperty")));
  }

  @Test
  public void doesNotRemoveEdgeTargetKeyMappingsFromEdgeMandatoryMappings() {
    target.setType(TargetType.edge);
    target.setName("edge target mappings do not overlap with rel mandatory (non-null) mappings");
    addMapping(
        target, mapping(FragmentType.target, RoleType.key, "source_field", "targetProperty"));
    addMapping(
        target,
        mandatoryMapping(FragmentType.rel, RoleType.property, "source_field", "targetProperty"));
    assertThat(target.getMappings()).hasSize(2);

    refactorer.refactorJobSpec(jobSpec);

    List<Mapping> actualMappings =
        jobSpec.getTargets().stream()
            .flatMap(t -> t.getMappings().stream())
            .collect(Collectors.toList());
    assertThat(actualMappings)
        .isEqualTo(
            List.of(
                mapping(FragmentType.target, RoleType.key, "source_field", "targetProperty"),
                mandatoryMapping(
                    FragmentType.rel, RoleType.property, "source_field", "targetProperty")));
  }

  @Test
  public void doesNotRemoveEdgeTargetKeyMappingsFromEdgeIndexedMappings() {
    target.setType(TargetType.edge);
    target.setName("edge target mappings do not overlap with rel mandatory (non-null) mappings");
    addMapping(
        target, mapping(FragmentType.target, RoleType.key, "source_field", "targetProperty"));
    addMapping(
        target,
        indexedMapping(FragmentType.rel, RoleType.property, "source_field", "targetProperty"));
    assertThat(target.getMappings()).hasSize(2);

    refactorer.refactorJobSpec(jobSpec);

    List<Mapping> actualMappings =
        jobSpec.getTargets().stream()
            .flatMap(t -> t.getMappings().stream())
            .collect(Collectors.toList());
    assertThat(actualMappings)
        .isEqualTo(
            List.of(
                mapping(FragmentType.target, RoleType.key, "source_field", "targetProperty"),
                indexedMapping(
                    FragmentType.rel, RoleType.property, "source_field", "targetProperty")));
  }

  @Test
  public void explicitlyMarksNodePropertyMappingsAsIndexableWhenApplicable() {
    target.setType(TargetType.node);
    target.setName("applies index-all-properties setting to every property");
    jobSpec.setConfig(new Config(new JSONObject(Map.of("index_all_properties", true))));
    addMapping(target, mapping(FragmentType.node, RoleType.key, "source_field", "targetProperty"));
    addMapping(target, mapping(FragmentType.node, RoleType.property, "field1", "prop1"));
    assertThat(target.getMappings()).hasSize(2);

    refactorer.refactorJobSpec(jobSpec);

    List<Mapping> actualMappings =
        jobSpec.getTargets().stream()
            .flatMap(t -> t.getMappings().stream())
            .collect(Collectors.toList());
    assertThat(actualMappings)
        .isEqualTo(
            List.of(
                mapping(FragmentType.node, RoleType.key, "source_field", "targetProperty"),
                indexedMapping(FragmentType.node, RoleType.property, "field1", "prop1")));
  }

  @Test
  public void explicitlyMarksRelPropertyMappingsAsIndexableWhenApplicable() {
    target.setType(TargetType.edge);
    target.setName("applies index-all-properties setting to every property");
    jobSpec.setConfig(new Config(new JSONObject(Map.of("index_all_properties", true))));
    addMapping(target, mapping(FragmentType.rel, RoleType.key, "source_field", "targetProperty"));
    addMapping(target, mapping(FragmentType.rel, RoleType.property, "field1", "prop1"));
    assertThat(target.getMappings()).hasSize(2);

    refactorer.refactorJobSpec(jobSpec);

    List<Mapping> actualMappings =
        jobSpec.getTargets().stream()
            .flatMap(t -> t.getMappings().stream())
            .collect(Collectors.toList());
    assertThat(actualMappings)
        .isEqualTo(
            List.of(
                mapping(FragmentType.rel, RoleType.key, "source_field", "targetProperty"),
                indexedMapping(FragmentType.rel, RoleType.property, "field1", "prop1")));
  }

  @Test
  public void removesInactiveTargets() {
    target.setType(TargetType.edge);
    target.setName("inactive target");
    target.setActive(false);
    addMapping(target, mapping(FragmentType.rel, RoleType.key, "source_field", "targetProperty"));
    assertThat(jobSpec.getTargets()).hasSize(1);

    refactorer.refactorJobSpec(jobSpec);

    assertThat(jobSpec.getTargets()).isEmpty();
  }

  @Test
  public void mergesOverlappingTargetMappings() {
    target.setType(TargetType.node);
    target.setName("different mappings for same property");
    addMapping(target, mapping(FragmentType.node, RoleType.key, "source_field", "targetProperty"));
    Mapping mapping =
        mapping(FragmentType.node, RoleType.property, "source_field", "targetProperty");
    mapping.setType(PropertyType.Boolean);
    addMapping(target, mapping);
    assertThat(target.getMappings()).hasSize(2);

    refactorer.refactorJobSpec(jobSpec);

    assertThat(target.getMappings()).hasSize(1);
    FieldNameTuple tuple = new FieldNameTuple();
    tuple.setName("targetProperty");
    tuple.setField("source_field");
    Mapping actualMapping = new Mapping(FragmentType.node, RoleType.key, tuple);
    actualMapping.setType(PropertyType.Boolean);
    List<Mapping> mappings = jobSpec.getTargets().iterator().next().getMappings();
    assertThat(mappings).isEqualTo(List.of(actualMapping));
  }

  private static Mapping uniqueMapping(
      FragmentType fragmentType, RoleType roleType, String column, String property) {
    Mapping mapping = mapping(fragmentType, roleType, column, property);
    mapping.setUnique(true);
    return mapping;
  }

  private static Mapping mandatoryMapping(
      FragmentType fragmentType, RoleType roleType, String column, String property) {
    Mapping mapping = mapping(fragmentType, roleType, column, property);
    mapping.setMandatory(true);
    return mapping;
  }

  private static Mapping indexedMapping(
      FragmentType fragmentType, RoleType roleType, String column, String property) {
    Mapping mapping = mapping(fragmentType, roleType, column, property);
    mapping.setIndexed(true);
    return mapping;
  }

  private static Mapping mapping(
      FragmentType fragmentType, RoleType roleType, String column, String property) {
    Mapping mapping = new Mapping();
    mapping.setFragmentType(fragmentType);
    mapping.setRole(roleType);
    mapping.setField(column);
    mapping.setName(property);
    return mapping;
  }

  private static void addMapping(Target target, Mapping mapping) {
    target.getFieldNames().add(mapping.getField());
    target.getMappings().add(mapping);
  }
}
