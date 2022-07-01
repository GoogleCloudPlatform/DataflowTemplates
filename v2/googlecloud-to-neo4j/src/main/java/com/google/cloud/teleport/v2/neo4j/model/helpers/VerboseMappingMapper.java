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
  public static Mapping fromJsonObject(final JSONObject mappingObj) {
    Mapping mapping = new Mapping();
    mapping.labels = Arrays.asList(mappingObj.has("label") ? mappingObj.getString("label") : "");
    mapping.constant = mappingObj.has("constant") ? mappingObj.getString("constant") : "";
    mapping.role =
        mappingObj.has("role") ? RoleType.valueOf(mappingObj.getString("role")) : mapping.role;
    mapping.fragmentType =
        mappingObj.has("fragment")
            ? FragmentType.valueOf(mappingObj.getString("fragment"))
            : mapping.fragmentType;

    mapping.field = mappingObj.has("field") ? mappingObj.getString("field") : "";
    mapping.name = mappingObj.has("name") ? mappingObj.getString("name") : "";
    if (StringUtils.isNotEmpty(mapping.field) && StringUtils.isEmpty(mapping.name)) {
      throw new RuntimeException("Invalid target.  Every field must include a 'name' attribute.");
    }
    // source value is required.
    mapping.description = mappingObj.has("description") ? mappingObj.getString("description") : "";
    mapping.unique = mappingObj.has("unique") && mappingObj.getBoolean("unique");
    mapping.indexed = mappingObj.has("indexed") && mappingObj.getBoolean("indexed");
    if (mapping.role == RoleType.key) {
      mapping.unique = true;
      mapping.indexed = true;
    }
    if (mappingObj.has("type")) {
      mapping.type = PropertyType.valueOf(mappingObj.getString("type"));
    } else {
      // check to see if data type is defined in fields...
      mapping.type = PropertyType.String;
    }
    mapping.mandatory = mappingObj.has("mandatory") && mappingObj.getBoolean("mandatory");
    mapping.defaultValue = mappingObj.has("default") ? mappingObj.get("default") + "" : "";
    return mapping;
  }
}
