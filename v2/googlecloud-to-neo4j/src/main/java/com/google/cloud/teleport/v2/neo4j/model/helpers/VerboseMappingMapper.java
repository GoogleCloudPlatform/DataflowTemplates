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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.PropertyType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

/** Helper object for parsing verbose field mappings. */
public class VerboseMappingMapper {

  public static Mapping fromJsonObject(JSONObject mappingObj) {
    Mapping mapping = new Mapping();
    mapping.setLabels(Arrays.asList(mappingObj.has("label") ? mappingObj.getString("label") : ""));
    mapping.setConstant(mappingObj.has("constant") ? mappingObj.getString("constant") : "");
    mapping.setRole(
        mappingObj.has("role")
            ? RoleType.valueOf(mappingObj.getString("role"))
            : mapping.getRole());
    mapping.setFragmentType(
        mappingObj.has("fragment")
            ? FragmentType.valueOf(mappingObj.getString("fragment"))
            : mapping.getFragmentType());

    mapping.setField(mappingObj.has("field") ? mappingObj.getString("field") : "");
    mapping.setName(mappingObj.has("name") ? mappingObj.getString("name") : "");
    if (StringUtils.isNotEmpty(mapping.getField()) && StringUtils.isEmpty(mapping.getName())) {
      throw new RuntimeException("Invalid target.  Every field must include a 'name' attribute.");
    }
    // source value is required.
    mapping.setDescription(
        mappingObj.has("description") ? mappingObj.getString("description") : "");
    mapping.setUnique(mappingObj.has("unique") && mappingObj.getBoolean("unique"));
    mapping.setIndexed(mappingObj.has("indexed") && mappingObj.getBoolean("indexed"));
    if (mapping.getRole() == RoleType.key) {
      mapping.setUnique(true);
      mapping.setIndexed(true);
    }
    if (mappingObj.has("type")) {
      mapping.setType(PropertyType.valueOf(mappingObj.getString("type")));
    } else {
      // check to see if data type is defined in fields...
      mapping.setType(PropertyType.String);
    }
    mapping.setMandatory(mappingObj.has("mandatory") && mappingObj.getBoolean("mandatory"));
    mapping.setDefaultValue(
        mappingObj.has("default") ? String.valueOf(mappingObj.get("default")) : "");
    return mapping;
  }
}
