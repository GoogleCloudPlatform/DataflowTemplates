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
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.FieldNameTuple;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

/** Helper object for parsing field mappings (ie. strings, indexed, longs, etc.). */
public class MappingMapper {

  public static List<Mapping> parseMappings(Target target, JSONObject mappingsObject) {
    String targetName = target.getName();
    MappingAccumulator accumulator = new MappingAccumulator(targetName);
    TargetType type = target.getType();
    switch (type) {
      case node:
        return parseNode(accumulator, mappingsObject);
      case edge:
        return parseEdge(accumulator, mappingsObject);
      default:
        String error =
            String.format(
                "Unknown target type for target %s: only \"node\" and \"edge\" types are supported, got: %s",
                targetName, type);
        throw new IllegalArgumentException(error);
    }
  }

  private static List<Mapping> parseNode(
      MappingAccumulator accumulator, JSONObject nodeMappingsObject) {
    if (nodeMappingsObject.has("label")) {
      FieldNameTuple labelTuple = createFieldNameTuple(nodeMappingsObject.getString("label"));
      accumulator.add(new Mapping(FragmentType.node, RoleType.label, labelTuple));
    }
    if (nodeMappingsObject.has("labels")) {
      List<FieldNameTuple> labels = getFieldAndNameTuples(nodeMappingsObject.get("labels"));
      for (FieldNameTuple f : labels) {
        Mapping mapping = new Mapping(FragmentType.node, RoleType.label, f);
        mapping.setIndexed(true);
        accumulator.add(mapping);
      }
    }
    if (nodeMappingsObject.has("key")) {
      FieldNameTuple tuple = createFieldNameTuple(nodeMappingsObject.getString("key"));
      accumulator.add(new Mapping(FragmentType.node, RoleType.key, tuple));
    }

    if (nodeMappingsObject.has("keys")) {
      List<FieldNameTuple> keys = getFieldAndNameTuples(nodeMappingsObject.get("keys"));
      for (FieldNameTuple f : keys) {
        Mapping mapping = new Mapping(FragmentType.node, RoleType.key, f);
        mapping.setIndexed(true);
        accumulator.add(mapping);
      }
    }
    if (nodeMappingsObject.has("properties")) {
      parseProperties(
          accumulator, nodeMappingsObject.getJSONObject("properties"), FragmentType.node);
    }
    return accumulator.getMappings();
  }

  private static List<Mapping> parseEdge(
      MappingAccumulator accumulator, JSONObject edgeMappingsObject) {
    if (edgeMappingsObject.has("type")) {
      FieldNameTuple typeTuple =
          createFieldNameTuple(
              edgeMappingsObject.getString("type"), edgeMappingsObject.getString("type"));
      accumulator.add(new Mapping(FragmentType.rel, RoleType.type, typeTuple));
    }

    if (edgeMappingsObject.has("source")) {
      parseEdgeNode(accumulator, FragmentType.source, edgeMappingsObject);
    }

    if (edgeMappingsObject.has("target")) {
      parseEdgeNode(accumulator, FragmentType.target, edgeMappingsObject);
    }

    if (edgeMappingsObject.has("keys")) {
      List<FieldNameTuple> keys = getFieldAndNameTuples(edgeMappingsObject.get("keys"));
      for (FieldNameTuple f : keys) {
        accumulator.add(new Mapping(FragmentType.rel, RoleType.key, f));
      }
    }

    if (edgeMappingsObject.has("properties")) {
      parseProperties(
          accumulator, edgeMappingsObject.getJSONObject("properties"), FragmentType.rel);
    }
    return accumulator.getMappings();
  }

  private static void parseEdgeNode(
      MappingAccumulator accumulator, FragmentType fragmentType, JSONObject edgeMappingsObject) {

    String fieldName = jsonPropertyNameForEdgeNode(fragmentType);
    JSONObject edgeNodeMapping = edgeMappingsObject.getJSONObject(fieldName);
    if (!edgeNodeMapping.has("key") && !edgeNodeMapping.has("keys")) {
      String error =
          String.format(
              "Edge node fragment of type %s should define a \"key\" or \"keys\" attribute. None found",
              fragmentType);
      throw new IllegalArgumentException(error);
    }
    if (edgeNodeMapping.has("key")) {
      List<FieldNameTuple> keyTuples = getFieldAndNameTuples(edgeNodeMapping.get("key"));
      for (FieldNameTuple keyTuple : keyTuples) {
        accumulator.add(new Mapping(fragmentType, RoleType.key, keyTuple));
      }
    }
    if (edgeNodeMapping.has("keys")) {
      List<FieldNameTuple> keyTuples = getFieldAndNameTuples(edgeNodeMapping.get("keys"));
      keyTuples.forEach(
          keyTuple -> {
            accumulator.add(new Mapping(fragmentType, RoleType.key, keyTuple));
          });
    }

    List<FieldNameTuple> labels = getFieldAndNameTuples(edgeNodeMapping.get("label"));
    for (FieldNameTuple f : labels) {
      accumulator.add(new Mapping(fragmentType, RoleType.label, f));
    }
  }

  private static String jsonPropertyNameForEdgeNode(FragmentType fragmentType) {
    switch (fragmentType) {
      case source:
        return "source";
      case target:
        return "target";
      default:
        String error =
            String.format(
                "Unexpected fragment type for edge mapping: expected \"source\" or \"target\", got: %s",
                fragmentType);
        throw new IllegalArgumentException(error);
    }
  }

  private static void parseProperties(
      MappingAccumulator accumulator, JSONObject propertyMappings, FragmentType fragmentType) {
    if (propertyMappings == null) {
      return;
    }

    List<FieldNameTuple> keys = new ArrayList<>();
    List<FieldNameTuple> uniques = new ArrayList<>();
    List<FieldNameTuple> indexed = new ArrayList<>();

    if (propertyMappings.has("keys")) {
      keys = getFieldAndNameTuples(propertyMappings.get("keys"));
    }
    if (propertyMappings.has("unique")) {
      uniques = getFieldAndNameTuples(propertyMappings.get("unique"));
    }
    if (propertyMappings.has("indexed")) {
      indexed = getFieldAndNameTuples(propertyMappings.get("indexed"));
    }

    for (FieldNameTuple key : keys) {
      accumulator.add(new Mapping(fragmentType, RoleType.key, key));
    }

    // TODO: remove keys from uniques
    for (FieldNameTuple f : uniques) {
      Mapping mapping = new Mapping(fragmentType, RoleType.property, f);
      mapping.setUnique(true);
      accumulator.add(mapping);
    }

    // TODO: remove keys and uniques from indexed since they're implicitly indexed
    for (FieldNameTuple f : indexed) {
      Mapping mapping = new Mapping(fragmentType, RoleType.property, f);
      mapping.setIndexed(true);
      accumulator.add(mapping);
    }
    if (propertyMappings.has("dates")) {
      List<FieldNameTuple> dates = getFieldAndNameTuples(propertyMappings.get("dates"));
      for (FieldNameTuple f : dates) {
        accumulator.add(newMapping(fragmentType, f, PropertyType.Date, indexed, uniques));
      }
    }
    if (propertyMappings.has("doubles")) {
      List<FieldNameTuple> doubles = getFieldAndNameTuples(propertyMappings.get("doubles"));
      for (FieldNameTuple f : doubles) {
        accumulator.add(newMapping(fragmentType, f, PropertyType.BigDecimal, indexed, uniques));
      }
    }
    if (propertyMappings.has("longs")) {
      List<FieldNameTuple> longs = getFieldAndNameTuples(propertyMappings.get("longs"));
      for (FieldNameTuple f : longs) {
        accumulator.add(newMapping(fragmentType, f, PropertyType.Long, indexed, uniques));
      }
    }
    if (propertyMappings.has("strings")) {
      List<FieldNameTuple> strings = getFieldAndNameTuples(propertyMappings.get("strings"));
      for (FieldNameTuple f : strings) {
        accumulator.add(newMapping(fragmentType, f, PropertyType.String, indexed, uniques));
      }
    }
    if (propertyMappings.has("points")) {
      List<FieldNameTuple> strings = getFieldAndNameTuples(propertyMappings.get("points"));
      for (FieldNameTuple f : strings) {
        accumulator.add(newMapping(fragmentType, f, PropertyType.Point, indexed, uniques));
      }
    }
    if (propertyMappings.has("floats")) {
      List<FieldNameTuple> floats = getFieldAndNameTuples(propertyMappings.get("floats"));
      for (FieldNameTuple f : floats) {
        accumulator.add(newMapping(fragmentType, f, PropertyType.Float, indexed, uniques));
      }
    }
    if (propertyMappings.has("integers")) {
      List<FieldNameTuple> integers = getFieldAndNameTuples(propertyMappings.get("integers"));
      for (FieldNameTuple f : integers) {
        accumulator.add(newMapping(fragmentType, f, PropertyType.Integer, indexed, uniques));
      }
    }
    if (propertyMappings.has("booleans")) {
      List<FieldNameTuple> booleans = getFieldAndNameTuples(propertyMappings.get("booleans"));
      for (FieldNameTuple f : booleans) {
        accumulator.add(newMapping(fragmentType, f, PropertyType.Boolean, indexed, uniques));
      }
    }
  }

  private static List<FieldNameTuple> getFieldAndNameTuples(Object tuplesObj) {
    List<FieldNameTuple> tuples = new ArrayList<>();
    if (tuplesObj instanceof JSONArray) {
      JSONArray tuplesArray = (JSONArray) tuplesObj;
      for (int i = 0; i < tuplesArray.length(); i++) {
        if (tuplesArray.get(i) instanceof JSONObject) {
          // {field:name} or {field1:name,field2:name} tuples
          Iterator<String> it = tuplesArray.getJSONObject(i).keys();
          while (it.hasNext()) {
            String key = it.next();
            tuples.add(createFieldNameTuple(key, tuplesArray.getJSONObject(i).getString(key)));
          }
        } else {
          tuples.add(createFieldNameTuple(tuplesArray.getString(i), tuplesArray.getString(i)));
        }
      }
    } else if (tuplesObj instanceof JSONObject) {
      JSONObject jsonObject = (JSONObject) tuplesObj;
      // {field:name} or {field1:name,field2:name} tuples
      Iterator<String> it = jsonObject.keys();
      while (it.hasNext()) {
        String key = it.next();
        tuples.add(createFieldNameTuple(key, jsonObject.getString(key)));
      }
    } else {
      tuples.add(createFieldNameTuple(String.valueOf(tuplesObj), String.valueOf(tuplesObj)));
    }
    return tuples;
  }

  private static FieldNameTuple createFieldNameTuple(String field) {
    return createFieldNameTuple(field, null);
  }

  private static FieldNameTuple createFieldNameTuple(String field, String name) {
    FieldNameTuple fieldSet = new FieldNameTuple();
    fieldSet.setName(name);
    field = field.trim();
    // handle double quoted constants
    if (field.charAt(0) == '\"' && field.charAt(field.length() - 1) == '\"') {
      fieldSet.setConstant(StringUtils.replace(field, "\"", ""));
      if (StringUtils.isEmpty(name)) {
        fieldSet.setName(fieldSet.getConstant());
      } else {
        fieldSet.setName(StringUtils.replace(name, "\"", ""));
      }
      // field is ""
    } else {
      if (StringUtils.isEmpty(name)) {
        fieldSet.setName(ModelUtils.makeValidNeo4jIdentifier(field));
      } else {
        fieldSet.setName(ModelUtils.makeValidNeo4jIdentifier(name));
      }
      fieldSet.setField(field);
    }
    return fieldSet;
  }

  private static Mapping newMapping(
      FragmentType fragmentType,
      FieldNameTuple tuple,
      PropertyType propertyType,
      List<FieldNameTuple> indexed,
      List<FieldNameTuple> uniques) {
    Mapping mapping = new Mapping(fragmentType, RoleType.property, tuple);
    mapping.setType(propertyType);
    mapping.setIndexed(indexed.contains(tuple));
    mapping.setUnique(uniques.contains(tuple));
    return mapping;
  }
}

class MappingAccumulator {
  private final Set<String> fields = new HashSet<>();
  private final List<Mapping> mappings = new ArrayList<>();
  private final String targetName;

  public MappingAccumulator(String targetName) {
    this.targetName = targetName;
  }

  public void add(Mapping mapping) {
    String field = mapping.getField();
    if (!StringUtils.isEmpty(field) && !fields.add(field)) {
      throw new IllegalArgumentException(
          String.format(
              "Duplicate mapping: field %s has already been mapped for target %s",
              field, targetName));
    }
    mappings.add(mapping);
  }

  public List<Mapping> getMappings() {
    return Collections.unmodifiableList(mappings);
  }
}
