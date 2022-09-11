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
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Field to Neo4j property mapping. */
@Getter
@Setter
public class Mapping implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(Mapping.class);
  private String constant;
  private RoleType role = RoleType.property;
  private PropertyType type;
  private String name = "";
  private List<String> labels = new ArrayList<>();
  private String field = "";
  private String description = "";
  private String defaultValue = "";
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

  public Mapping(String name, PropertyType type) {
    this.name = name;
    this.type = type;
  }
}
