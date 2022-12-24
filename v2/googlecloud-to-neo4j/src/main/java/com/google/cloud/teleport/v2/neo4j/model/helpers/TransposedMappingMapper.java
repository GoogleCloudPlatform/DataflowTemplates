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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper object for parsing transposed field mappings (ie. strings, indexed, longs, etc.). */
public class TransposedMappingMapper {

  private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
  private static final Logger LOG = LoggerFactory.getLogger(TransposedMappingMapper.class);

  public static List<Mapping> parseMappings(Target target, JSONObject mappingsObject) {
    if (target.getType() == TargetType.node) {
      return parseNode(mappingsObject);
    } else if (target.getType() == TargetType.edge) {
      return parseEdge(mappingsObject);
    } else {
      return new ArrayList<>();
    }
  }

  public static List<Mapping> parseNode(JSONObject nodeMappingsObject) {
    List<Mapping> mappings = new ArrayList<>();

    if (nodeMappingsObject.has("label")) {
      FieldNameTuple labelTuple = createFieldNameTuple(nodeMappingsObject.getString("label"));
      Mapping mapping = new Mapping(FragmentType.node, RoleType.label, labelTuple);
      addMapping(mappings, mapping);
    }
    if (nodeMappingsObject.has("labels")) {
      List<FieldNameTuple> labels = getFieldAndNameTuples(nodeMappingsObject.get("labels"));
      for (FieldNameTuple f : labels) {
        Mapping mapping = new Mapping(FragmentType.node, RoleType.label, f);
        mapping.setIndexed(true);
        addMapping(mappings, mapping);
      }
    }
    if (nodeMappingsObject.has("key")) {
      FieldNameTuple labelTuple = createFieldNameTuple(nodeMappingsObject.getString("key"));
      Mapping mapping = new Mapping(FragmentType.node, RoleType.key, labelTuple);
      addMapping(mappings, mapping);
    }

    if (nodeMappingsObject.has("keys")) {
      List<FieldNameTuple> keys = getFieldAndNameTuples(nodeMappingsObject.get("keys"));
      for (FieldNameTuple f : keys) {
        Mapping mapping = new Mapping(FragmentType.node, RoleType.key, f);
        mapping.setIndexed(true);
        addMapping(mappings, mapping);
      }
    }
    if (nodeMappingsObject.has("properties")) {
      parseProperties(nodeMappingsObject.getJSONObject("properties"), mappings, FragmentType.node);
    }
    return mappings;
  }

  public static List<Mapping> parseEdge(JSONObject edgeMappingsObject) {
    List<Mapping> mappings = new ArrayList<>();
    // type
    if (edgeMappingsObject.has("type")) {
      FieldNameTuple typeTuple =
          createFieldNameTuple(
              edgeMappingsObject.getString("type"), edgeMappingsObject.getString("type"));
      Mapping mapping = new Mapping(FragmentType.rel, RoleType.type, typeTuple);
      addMapping(mappings, mapping);
    }

    if (edgeMappingsObject.has("source")) {
      JSONObject sourceObj = edgeMappingsObject.getJSONObject("source");
      FieldNameTuple keyTuple = getFieldAndNameTuple(sourceObj.get("key"));
      Mapping keyMapping = new Mapping(FragmentType.source, RoleType.key, keyTuple);
      // List<String> labels = getLabels(sourceObj.getString("label").trim());
      // keyMapping.setLabels(labels);
      mappings.add(keyMapping);

      // support dynamic labels on source
      List<FieldNameTuple> labels = getFieldAndNameTuples(sourceObj.get("label"));
      for (FieldNameTuple f : labels) {
        Mapping mapping = new Mapping(FragmentType.source, RoleType.label, f);
        mapping.setIndexed(true);
        addMapping(mappings, mapping);
      }
    }

    if (edgeMappingsObject.has("target")) {
      JSONObject sourceObj = edgeMappingsObject.getJSONObject("target");
      FieldNameTuple keyTuple = getFieldAndNameTuple(sourceObj.get("key"));
      Mapping keyMapping = new Mapping(FragmentType.target, RoleType.key, keyTuple);

      // List<String> labels = getLabels(sourceObj.getString("label").trim());
      // keyMapping.setLabels(labels);
      mappings.add(keyMapping);

      List<FieldNameTuple> labels = getFieldAndNameTuples(sourceObj.get("label"));
      for (FieldNameTuple f : labels) {
        Mapping mapping = new Mapping(FragmentType.target, RoleType.label, f);
        mapping.setIndexed(true);
        addMapping(mappings, mapping);
      }
    }
    if (edgeMappingsObject.has("properties")) {
      parseProperties(edgeMappingsObject.getJSONObject("properties"), mappings, FragmentType.rel);
    }
    return mappings;
  }

  private static void parseProperties(
      JSONObject propertyMappingsObject, List<Mapping> mappings, FragmentType fragmentType) {
    if (propertyMappingsObject == null) {
      return;
    }

    List<FieldNameTuple> uniques = new ArrayList();
    List<FieldNameTuple> indexed = new ArrayList<>();

    if (propertyMappingsObject.has("unique")) {
      uniques = getFieldAndNameTuples(propertyMappingsObject.get("unique"));
    }
    if (propertyMappingsObject.has("indexed")) {
      indexed = getFieldAndNameTuples(propertyMappingsObject.get("indexed"));
    }

    for (FieldNameTuple f : uniques) {
      Mapping mapping = new Mapping(fragmentType, RoleType.property, f);
      addMapping(mappings, mapping);
      mapping.setIndexed(indexed.contains(f));
    }
    for (FieldNameTuple f : indexed) {
      Mapping mapping = new Mapping(fragmentType, RoleType.property, f);
      addMapping(mappings, mapping);
      mapping.setUnique(uniques.contains(f));
    }
    if (propertyMappingsObject.has("dates")) {
      List<FieldNameTuple> dates = getFieldAndNameTuples(propertyMappingsObject.get("dates"));
      for (FieldNameTuple f : dates) {
        Mapping mapping = new Mapping(fragmentType, RoleType.property, f);
        mapping.setType(PropertyType.Date);
        mapping.setIndexed(indexed.contains(f));
        mapping.setUnique(uniques.contains(f));
        addMapping(mappings, mapping);
      }
    }

    if (propertyMappingsObject.has("doubles")) {

      List<FieldNameTuple> numbers = getFieldAndNameTuples(propertyMappingsObject.get("doubles"));
      for (FieldNameTuple f : numbers) {
        Mapping mapping = new Mapping(fragmentType, RoleType.property, f);
        mapping.setType(PropertyType.BigDecimal);
        mapping.setIndexed(indexed.contains(f));
        mapping.setUnique(uniques.contains(f));
        addMapping(mappings, mapping);
      }
    }
    if (propertyMappingsObject.has("longs")) {
      List<FieldNameTuple> longs = getFieldAndNameTuples(propertyMappingsObject.get("longs"));
      for (FieldNameTuple f : longs) {
        Mapping mapping = new Mapping(fragmentType, RoleType.property, f);
        mapping.setType(PropertyType.Long);
        mapping.setIndexed(indexed.contains(f));
        mapping.setUnique(uniques.contains(f));
        addMapping(mappings, mapping);
      }
    }
    if (propertyMappingsObject.has("strings")) {
      List<FieldNameTuple> strings = getFieldAndNameTuples(propertyMappingsObject.get("strings"));
      for (FieldNameTuple f : strings) {
        Mapping mapping = new Mapping(fragmentType, RoleType.property, f);
        mapping.setType(PropertyType.String);
        mapping.setIndexed(indexed.contains(f));
        mapping.setUnique(uniques.contains(f));
        addMapping(mappings, mapping);
      }
    }
    if (propertyMappingsObject.has("points")) {
      List<FieldNameTuple> strings = getFieldAndNameTuples(propertyMappingsObject.get("points"));
      for (FieldNameTuple f : strings) {
        Mapping mapping = new Mapping(fragmentType, RoleType.property, f);
        mapping.setType(PropertyType.Point);
        mapping.setIndexed(indexed.contains(f));
        mapping.setUnique(uniques.contains(f));
        addMapping(mappings, mapping);
      }
    }
    if (propertyMappingsObject.has("floats")) {
      List<FieldNameTuple> strings = getFieldAndNameTuples(propertyMappingsObject.get("floats"));
      for (FieldNameTuple f : strings) {
        Mapping mapping = new Mapping(fragmentType, RoleType.property, f);
        mapping.setType(PropertyType.Float);
        mapping.setIndexed(indexed.contains(f));
        mapping.setUnique(uniques.contains(f));
        addMapping(mappings, mapping);
      }
    }
    if (propertyMappingsObject.has("integers")) {
      List<FieldNameTuple> strings = getFieldAndNameTuples(propertyMappingsObject.get("integers"));
      for (FieldNameTuple f : strings) {
        Mapping mapping = new Mapping(fragmentType, RoleType.property, f);
        mapping.setType(PropertyType.Integer);
        mapping.setIndexed(indexed.contains(f));
        mapping.setUnique(uniques.contains(f));
        addMapping(mappings, mapping);
      }
    }
  }

  private static List<String> getLabels(Object tuplesObj) {
    List<String> labels = new ArrayList<>();
    if (tuplesObj instanceof JSONArray) {
      JSONArray tuplesArray = (JSONArray) tuplesObj;
      for (int i = 0; i < tuplesArray.length(); i++) {
        if (tuplesArray.get(i) instanceof JSONObject) {
          // {field:name} or {field1:name,field2:name} tuples
          Iterator<String> it = tuplesArray.getJSONObject(i).keys();
          while (it.hasNext()) {
            String key = it.next();
            labels.add(key);
          }
        } else {
          labels.add(tuplesArray.getString(i));
        }
      }
    } else if (tuplesObj instanceof JSONObject) {
      JSONObject jsonObject = (JSONObject) tuplesObj;
      // {field:name} or {field1:name,field2:name} tuples
      Iterator<String> it = jsonObject.keys();
      while (it.hasNext()) {
        labels.add(it.next());
      }
    } else {
      labels.add(String.valueOf(tuplesObj));
    }
    return labels;
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

  private static FieldNameTuple getFieldAndNameTuple(Object tuplesObj) {
    FieldNameTuple tuple = new FieldNameTuple();
    if (tuplesObj instanceof JSONObject) {
      JSONObject jsonObject = (JSONObject) tuplesObj;
      // {field:name} or {field1:name,field2:name} tuples
      Iterator<String> it = jsonObject.keys();
      while (it.hasNext()) {
        String key = it.next();
        tuple = createFieldNameTuple(key, jsonObject.getString(key));
      }
    } else {
      tuple = createFieldNameTuple(String.valueOf(tuplesObj), String.valueOf(tuplesObj));
    }
    return tuple;
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

  private static void addMapping(List<Mapping> mappings, Mapping mapping) {
    if (!StringUtils.isEmpty(mapping.getField())) {
      for (Mapping existingMapping : mappings) {
        if (existingMapping.getField() != null
            && existingMapping.getField().equals(mapping.getField())) {
          throw new RuntimeException("Duplicate mapping: " + gson.toJson(mapping));
        }
      }
    }
    mappings.add(mapping);
  }
}
