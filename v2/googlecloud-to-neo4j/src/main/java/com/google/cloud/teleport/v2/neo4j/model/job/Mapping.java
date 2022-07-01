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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Field to Neo4j property mapping. */
public class Mapping implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(Mapping.class);
  public String constant;
  public RoleType role = RoleType.property;
  public PropertyType type;
  public String name = "";
  public List<String> labels = new ArrayList();
  public String field = "";
  public String description = "";
  public String defaultValue = "";
  public boolean mandatory = false;
  public boolean unique = false;
  public boolean indexed = true;
  public FragmentType fragmentType = FragmentType.node;

  public Mapping() {}

  public Mapping(FragmentType fragmentType, RoleType type, FieldNameTuple fieldNameTuple) {
    this.role = type;
    this.fragmentType = fragmentType;
    this.name = fieldNameTuple.name;
    this.field = fieldNameTuple.field;
    this.constant = fieldNameTuple.constant;
  }
}
