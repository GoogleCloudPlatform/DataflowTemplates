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
package com.google.cloud.teleport.v2.neo4j.model.job;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.PropertyType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import org.junit.Test;

public class MappingTest {

  @Test
  public void mergesWithKeyRolePrecedence() {
    FieldNameTuple fieldMapping = placeholderFieldMapping();
    Mapping mapping1 = new Mapping(FragmentType.node, RoleType.key, fieldMapping);
    Mapping mapping2 = new Mapping(FragmentType.node, RoleType.property, fieldMapping);

    Mapping result1 = mapping1.mergeOverlapping(mapping2);
    Mapping result2 = mapping2.mergeOverlapping(mapping1);

    assertThat(result1).isEqualTo(result2);
    assertThat(result1.getRole()).isEqualTo(RoleType.key);
    assertThat(result1.getFragmentType()).isEqualTo(FragmentType.node);
    assertThat(result1.getName()).isEqualTo(fieldMapping.getName());
    assertThat(result1.getField()).isEqualTo(fieldMapping.getField());
    assertThat(result1.getConstant()).isEqualTo(fieldMapping.getConstant());
    assertThat(result1.getType()).isNull();
  }

  @Test
  public void mergesWithKeyRoleResettingIndexConstraintAttributes() {
    FieldNameTuple fieldMapping = placeholderFieldMapping();
    Mapping mapping1 = new Mapping(FragmentType.node, RoleType.key, fieldMapping);
    Mapping mapping2 = new Mapping(FragmentType.node, RoleType.property, fieldMapping);
    mapping2.setUnique(true);
    Mapping mapping3 = new Mapping(FragmentType.node, RoleType.property, fieldMapping);
    mapping3.setMandatory(true);
    mapping3.setIndexed(true);

    Mapping result1 = mapping1.mergeOverlapping(mapping2);
    Mapping result2 = mapping3.mergeOverlapping(mapping1);

    assertThat(result1.isIndexed()).isFalse();
    assertThat(result1.isMandatory()).isFalse();
    assertThat(result1.isUnique()).isFalse();
    assertThat(result2.isIndexed()).isFalse();
    assertThat(result2.isMandatory()).isFalse();
    assertThat(result2.isUnique()).isFalse();
  }

  @Test
  public void mergesWithUniqueness() {
    FieldNameTuple fieldMapping = placeholderFieldMapping();
    Mapping mapping1 = new Mapping(FragmentType.node, RoleType.property, fieldMapping);
    mapping1.setUnique(true);
    Mapping mapping2 = new Mapping(FragmentType.node, RoleType.property, fieldMapping);

    Mapping result1 = mapping1.mergeOverlapping(mapping2);
    Mapping result2 = mapping2.mergeOverlapping(mapping1);

    assertThat(result1).isEqualTo(result2);
    assertThat(result1.isUnique()).isTrue();
  }

  @Test
  public void mergesWithUniquenessResettingIndexAttribute() {
    FieldNameTuple fieldMapping = placeholderFieldMapping();
    Mapping mapping1 = new Mapping(FragmentType.node, RoleType.property, fieldMapping);
    mapping1.setUnique(true);
    Mapping mapping2 = new Mapping(FragmentType.node, RoleType.property, fieldMapping);
    mapping2.setIndexed(true);

    Mapping result = mapping1.mergeOverlapping(mapping2);

    assertThat(result.isIndexed()).isFalse();
  }

  @Test
  public void mergesWithMandatory() {
    FieldNameTuple fieldMapping = placeholderFieldMapping();
    Mapping mapping1 = new Mapping(FragmentType.node, RoleType.property, fieldMapping);
    mapping1.setMandatory(true);
    Mapping mapping2 = new Mapping(FragmentType.node, RoleType.property, fieldMapping);

    Mapping result1 = mapping1.mergeOverlapping(mapping2);
    Mapping result2 = mapping2.mergeOverlapping(mapping1);

    assertThat(result1).isEqualTo(result2);
    assertThat(result1.isMandatory()).isTrue();
  }

  @Test
  public void mergesWithIndexed() {
    FieldNameTuple fieldMapping = placeholderFieldMapping();
    Mapping mapping1 = new Mapping(FragmentType.node, RoleType.property, fieldMapping);
    mapping1.setIndexed(true);
    Mapping mapping2 = new Mapping(FragmentType.node, RoleType.property, fieldMapping);

    Mapping result1 = mapping1.mergeOverlapping(mapping2);
    Mapping result2 = mapping2.mergeOverlapping(mapping1);

    assertThat(result1).isEqualTo(result2);
    assertThat(result1.isIndexed()).isTrue();
  }

  @Test
  public void mergesWithTypes() {
    FieldNameTuple fieldMapping = placeholderFieldMapping();
    Mapping mapping1 = new Mapping(FragmentType.node, RoleType.property, fieldMapping);
    mapping1.setType(PropertyType.Boolean);
    Mapping mapping2 = new Mapping(FragmentType.node, RoleType.property, fieldMapping);

    Mapping result1 = mapping1.mergeOverlapping(mapping2);
    Mapping result2 = mapping2.mergeOverlapping(mapping1);

    assertThat(result1).isEqualTo(result2);
    assertThat(result1.getType()).isEqualTo(PropertyType.Boolean);
  }

  private static FieldNameTuple placeholderFieldMapping() {
    FieldNameTuple tuple = new FieldNameTuple();
    tuple.setField("placeholder_field");
    tuple.setName("placeholder_property");
    return tuple;
  }
}
