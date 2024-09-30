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
package com.google.cloud.teleport.v2.neo4j.database;

import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.cypherdsl.support.schema_name.SchemaNames;
import org.neo4j.importer.v1.targets.EntityTarget;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.PropertyType;

public class CypherPatterns {

  private final String keyPropertiesPattern;
  private final String nonKeyPropertiesSet;

  private CypherPatterns(String keyPropertiesPattern, String nonKeyPropertiesSet) {
    this.keyPropertiesPattern = keyPropertiesPattern;
    this.nonKeyPropertiesSet = nonKeyPropertiesSet;
  }

  public static CypherPatterns parsePatterns(
      EntityTarget entity, String entityVariable, String rowVariable) {
    Set<String> keyProperties = new LinkedHashSet<>(entity.getKeyProperties());
    String cypherKeyProperties = assignPropertiesInPattern(entity, keyProperties, rowVariable);
    List<String> nonKeyProperties = new ArrayList<>(entity.getAllProperties());
    nonKeyProperties.removeAll(keyProperties);
    String cypherSetNonKeys =
        assignProperties(entity, nonKeyProperties, entityVariable, rowVariable, "SET ", " = ");
    return new CypherPatterns(cypherKeyProperties, cypherSetNonKeys);
  }

  public static String schemaOptions(Map<String, Object> options) {
    if (options == null) {
      return "";
    }
    return "OPTIONS " + optionsAsMap(options);
  }

  @SuppressWarnings("unchecked")
  private static String schemaOption(Object value) {
    if (value instanceof Map) {
      return optionsAsMap((Map<String, Object>) value);
    }
    if (value instanceof Collection<?>) {
      return optionsAsList((Collection<?>) value);
    }
    if (value instanceof String) {
      return String.format("'%s'", value);
    }
    return String.valueOf(value);
  }

  private static String optionsAsMap(Map<String, Object> options) {
    return options.entrySet().stream()
        .map(
            entry ->
                String.format("%s: %s", sanitize(entry.getKey()), schemaOption(entry.getValue())))
        .collect(Collectors.joining(", ", "{", "}"));
  }

  private static String optionsAsList(Collection<?> value) {
    return value.stream()
        .map(CypherPatterns::schemaOption)
        .collect(Collectors.joining(",", "[", "]"));
  }

  public static String propertyType(PropertyType propertyType) {
    switch (propertyType) {
      case BOOLEAN:
        return "BOOLEAN";
      case BOOLEAN_ARRAY:
        return "LIST<BOOLEAN NOT NULL>";
      case DATE:
        return "DATE";
      case DATE_ARRAY:
        return "LIST<DATE NOT NULL>";
      case DURATION:
        return "DURATION";
      case DURATION_ARRAY:
        return "LIST<DURATION NOT NULL>";
      case FLOAT:
        return "FLOAT";
      case FLOAT_ARRAY:
        return "LIST<FLOAT NOT NULL>";
      case INTEGER:
        return "INTEGER";
      case INTEGER_ARRAY:
        return "LIST<INTEGER NOT NULL>";
      case LOCAL_DATETIME:
        return "LOCAL DATETIME";
      case LOCAL_DATETIME_ARRAY:
        return "LIST<LOCAL DATETIME NOT NULL>";
      case LOCAL_TIME:
        return "LOCAL TIME";
      case LOCAL_TIME_ARRAY:
        return "LIST<LOCAL TIME NOT NULL>";
      case POINT:
        return "POINT";
      case POINT_ARRAY:
        return "LIST<POINT NOT NULL>";
      case STRING:
        return "STRING";
      case STRING_ARRAY:
        return "LIST<STRING NOT NULL>";
      case ZONED_DATETIME:
        return "ZONED DATETIME";
      case ZONED_DATETIME_ARRAY:
        return "LIST<ZONED DATETIME NOT NULL>";
      case ZONED_TIME:
        return "ZONED TIME";
      case ZONED_TIME_ARRAY:
        return "LIST<ZONED TIME NOT NULL>";
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported property type: %s", propertyType));
    }
  }

  public String keysPattern() {
    return keyPropertiesPattern;
  }

  public String nonKeysSetClause() {
    return nonKeyPropertiesSet;
  }

  public static String labels(List<String> labels) {
    return labels(labels, ":");
  }

  public static String labels(List<String> labels, String separator) {
    return labels.stream()
        .map(CypherPatterns::sanitize)
        .collect(Collectors.joining(separator, ":", ""));
  }

  private static String assignPropertiesInPattern(
      EntityTarget target, Collection<String> properties, String rowVariable) {
    return assignProperties(target, properties, "", rowVariable, "", ": ");
  }

  private static String assignProperties(
      EntityTarget target,
      Collection<String> properties,
      String entityVariable,
      String rowVariable,
      String prefix,
      String separator) {

    if (properties.isEmpty()) {
      return "";
    }
    Map<String, String> fieldsByProperty =
        target.getProperties().stream()
            .collect(toMap(PropertyMapping::getTargetProperty, PropertyMapping::getSourceField));

    return properties.stream()
        .map(
            property -> {
              String escapedQualifiedProperty = qualify(property, entityVariable);
              return String.format(
                  "%s%s%s.`%s`",
                  escapedQualifiedProperty, separator, rowVariable, fieldsByProperty.get(property));
            })
        .collect(Collectors.joining(", ", prefix, ""));
  }

  private static String qualify(String variable, String entityVariable) {
    String escapedProperty = sanitize(variable);
    if (entityVariable.isEmpty()) {
      return escapedProperty;
    }
    return prefixWith(entityVariable + ".", escapedProperty);
  }

  public static Collection<String> qualifyAll(
      String variable, Collection<String> escapedProperties) {
    return prefixAllWith(variable + ".", escapedProperties);
  }

  private static Collection<String> prefixAllWith(String prefix, Collection<String> elements) {
    return elements.stream()
        .map(element -> prefixWith(prefix, element))
        .collect(Collectors.toList());
  }

  private static String prefixWith(String prefix, String element) {
    return String.format("%s%s", prefix, element);
  }

  public static List<String> sanitizeAll(Collection<String> properties) {
    return properties.stream().map(CypherPatterns::sanitize).collect(Collectors.toList());
  }

  public static String sanitize(String identifier) {
    return SchemaNames.sanitize(identifier, true)
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format("cannot sanitize identifier %s", identifier)));
  }
}
