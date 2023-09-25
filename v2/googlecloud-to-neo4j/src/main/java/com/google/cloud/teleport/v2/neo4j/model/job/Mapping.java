/*
 * Copyright (C) 2021 Google LLC
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

import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.PropertyType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Field to Neo4j property mapping. */
public class Mapping implements Serializable {

  private String constant;
  private RoleType role = RoleType.property;
  private PropertyType type;
  private String name = "";
  private List<String> labels = new ArrayList<>();
  private String field = "";
  private String description = "";
  private boolean mandatory = false;
  private boolean unique = false;
  private boolean indexed = true;
  private FragmentType fragmentType = FragmentType.node;

  public Mapping() {}

  public Mapping(FragmentType fragmentType, RoleType type, FieldNameTuple fieldNameTuple) {
    this.role = type;
    this.fragmentType = fragmentType;
    this.name = fieldNameTuple.getName();
    this.field = fieldNameTuple.getField();
    this.constant = fieldNameTuple.getConstant();
  }

  public void setLabels(List<String> labels) {}

  public String getConstant() {
    return constant;
  }

  public void setConstant(String constant) {
    this.constant = constant;
  }

  public RoleType getRole() {
    return role;
  }

  public void setRole(RoleType role) {
    this.role = role;
  }

  public PropertyType getType() {
    return type;
  }

  public void setType(PropertyType type) {
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<String> getLabels() {
    return labels;
  }

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public boolean isMandatory() {
    return mandatory;
  }

  public void setMandatory(boolean mandatory) {
    this.mandatory = mandatory;
  }

  public boolean isUnique() {
    return unique;
  }

  public void setUnique(boolean unique) {
    this.unique = unique;
  }

  public boolean isIndexed() {
    return indexed;
  }

  public void setIndexed(boolean indexed) {
    this.indexed = indexed;
  }

  public FragmentType getFragmentType() {
    return fragmentType;
  }

  public void setFragmentType(FragmentType fragmentType) {
    this.fragmentType = fragmentType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Mapping mapping = (Mapping) o;
    return mandatory == mapping.mandatory
        && unique == mapping.unique
        && indexed == mapping.indexed
        && Objects.equals(constant, mapping.constant)
        && role == mapping.role
        && type == mapping.type
        && Objects.equals(name, mapping.name)
        && Objects.equals(labels, mapping.labels)
        && Objects.equals(field, mapping.field)
        && Objects.equals(description, mapping.description)
        && fragmentType == mapping.fragmentType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        constant,
        role,
        type,
        name,
        labels,
        field,
        description,
        mandatory,
        unique,
        indexed,
        fragmentType);
  }

  @Override
  public String toString() {
    return "Mapping{"
        + "constant='"
        + constant
        + '\''
        + ", role="
        + role
        + ", type="
        + type
        + ", name='"
        + name
        + '\''
        + ", labels="
        + labels
        + ", field='"
        + field
        + '\''
        + ", description='"
        + description
        + '\''
        + ", mandatory="
        + mandatory
        + ", unique="
        + unique
        + ", indexed="
        + indexed
        + ", fragmentType="
        + fragmentType
        + '}';
  }
}
