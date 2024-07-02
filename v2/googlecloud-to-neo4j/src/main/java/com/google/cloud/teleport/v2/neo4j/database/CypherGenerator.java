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
package com.google.cloud.teleport.v2.neo4j.database;

import static com.google.cloud.teleport.v2.neo4j.database.CypherPatterns.escape;
import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.targets.EntityTarget;
import org.neo4j.importer.v1.targets.NodeSchema;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.PropertyType;
import org.neo4j.importer.v1.targets.RelationshipSchema;
import org.neo4j.importer.v1.targets.RelationshipTarget;

/** Generates cypher based on model metadata. */
public class CypherGenerator {
  private static final String ROWS_VARIABLE_NAME = "rows";
  private static final String ROW_VARIABLE_NAME = "row";

  /**
   * getImportStatement generates the batch import statement of the specified node or relationship
   * target.
   *
   * @param importSpecification the whole import specification
   * @param target the node or relationship target
   * @return the batch import query
   */
  public static String getImportStatement(
      ImportSpecification importSpecification, EntityTarget target) {
    var type = target.getTargetType();
    switch (type) {
      case NODE:
        return unwindNodes((NodeTarget) target);
      case RELATIONSHIP:
        return unwindRelationships(importSpecification, (RelationshipTarget) target);
      default:
        throw new IllegalArgumentException(String.format("unexpected target type: %s", type));
    }
  }

  /**
   * getSchemaStatements generates the Cypher schema statements for the specified node or
   * relationship target.
   *
   * @return a collection of Cypher schema statements
   */
  public static Set<String> getSchemaStatements(
      EntityTarget target, Neo4jCapabilities capabilities) {
    var type = target.getTargetType();
    switch (type) {
      case NODE:
        return getNodeSchemaStatements((NodeTarget) target, capabilities);
      case RELATIONSHIP:
        return getRelationshipSchemaStatements((RelationshipTarget) target, capabilities);
      default:
        throw new IllegalArgumentException(String.format("unexpected target type: %s", type));
    }
  }

  private static String unwindNodes(NodeTarget nodeTarget) {
    String cypherLabels = CypherPatterns.labels(nodeTarget.getLabels());
    CypherPatterns patterns = CypherPatterns.parsePatterns(nodeTarget, "n", ROW_VARIABLE_NAME);

    return "UNWIND $"
        + ROWS_VARIABLE_NAME
        + " AS "
        + ROW_VARIABLE_NAME
        + " "
        + nodeTarget.getWriteMode().name()
        + " (n"
        + cypherLabels
        + " {"
        + patterns.keysPattern()
        + "}) "
        + patterns.nonKeysSetClause();
  }

  private static String unwindRelationships(
      ImportSpecification importSpecification, RelationshipTarget relationship) {
    String nodeClause = relationship.getNodeMatchMode().name();
    NodeTarget startNode =
        resolveRelationshipNode(importSpecification, relationship.getStartNodeReference());
    String startNodeKeys =
        CypherPatterns.parsePatterns(startNode, "start", ROW_VARIABLE_NAME).keysPattern();
    NodeTarget endNode =
        resolveRelationshipNode(importSpecification, relationship.getEndNodeReference());
    String endNodeKeys =
        CypherPatterns.parsePatterns(endNode, "end", ROW_VARIABLE_NAME).keysPattern();
    String relationshipClause = relationship.getWriteMode().name();
    CypherPatterns relationshipPatterns =
        CypherPatterns.parsePatterns(relationship, "r", ROW_VARIABLE_NAME);

    String relationshipKeysPattern = relationshipPatterns.keysPattern();
    String relationshipNonKeysClause = relationshipPatterns.nonKeysSetClause();
    return "UNWIND $"
        + ROWS_VARIABLE_NAME
        + " AS "
        + ROW_VARIABLE_NAME
        + " "
        + nodeClause
        + " (start"
        + CypherPatterns.labels(startNode.getLabels())
        + (startNodeKeys.isEmpty() ? "" : " {" + startNodeKeys + "}")
        + ")"
        + " "
        + nodeClause
        + " (end"
        + CypherPatterns.labels(endNode.getLabels())
        + (endNodeKeys.isEmpty() ? "" : " {" + endNodeKeys + "}")
        + ")"
        + " "
        + relationshipClause
        + " (start)-[r:"
        + escape(relationship.getType())
        + (relationshipKeysPattern.isEmpty() ? "" : " {" + relationshipKeysPattern + "}")
        + "]->(end)"
        + (relationshipNonKeysClause.isEmpty() ? "" : " " + relationshipNonKeysClause);
  }

  private static Set<String> getNodeSchemaStatements(
      NodeTarget target, Neo4jCapabilities capabilities) {
    NodeSchema schema = target.getSchema();
    Set<String> statements = new LinkedHashSet<>();

    if (capabilities.hasNodeTypeConstraints()) {
      Map<String, PropertyType> types = indexPropertyTypes(target.getProperties());
      for (var constraint : schema.getTypeConstraints()) {
        String property = constraint.getProperty();
        statements.add(
            "CREATE CONSTRAINT "
                + escape(constraint.getName())
                + " IF NOT EXISTS FOR (n:"
                + escape(constraint.getLabel())
                + ") REQUIRE n."
                + escape(property)
                + " IS :: "
                + CypherPatterns.propertyType(types.get(property)));
      }
    }
    if (capabilities.hasNodeKeyConstraints()) {
      for (var constraint : schema.getKeyConstraints()) {
        var properties =
            CypherPatterns.qualifyAll("n", CypherPatterns.escapeAll(constraint.getProperties()));
        var options = CypherPatterns.schemaOptions(constraint.getOptions());
        statements.add(
            "CREATE CONSTRAINT "
                + escape(constraint.getName())
                + " IF NOT EXISTS FOR (n:"
                + escape(constraint.getLabel())
                + ") REQUIRE ("
                + String.join(", ", properties)
                + ") IS NODE KEY"
                + (options.isEmpty() ? "" : " " + options));
      }
    }
    if (capabilities.hasNodeUniqueConstraints()) {
      for (var constraint : schema.getUniqueConstraints()) {
        var properties =
            CypherPatterns.qualifyAll("n", CypherPatterns.escapeAll(constraint.getProperties()));
        var options = CypherPatterns.schemaOptions(constraint.getOptions());
        statements.add(
            "CREATE CONSTRAINT "
                + escape(constraint.getName())
                + " IF NOT EXISTS FOR (n:"
                + escape(constraint.getLabel())
                + ") REQUIRE ("
                + String.join(", ", properties)
                + ") IS UNIQUE"
                + (options.isEmpty() ? "" : " " + options));
      }
    }
    if (capabilities.hasNodeExistenceConstraints()) {
      for (var constraint : schema.getExistenceConstraints()) {
        statements.add(
            "CREATE CONSTRAINT "
                + escape(constraint.getName())
                + " IF NOT EXISTS FOR (n:"
                + escape(constraint.getLabel())
                + ") REQUIRE n."
                + escape(constraint.getProperty())
                + " IS NOT NULL");
      }
    }
    for (var index : schema.getRangeIndexes()) {
      var properties =
          CypherPatterns.qualifyAll("n", CypherPatterns.escapeAll(index.getProperties()));
      statements.add(
          "CREATE INDEX "
              + escape(index.getName())
              + " IF NOT EXISTS FOR (n:"
              + escape(index.getLabel())
              + ") ON ("
              + String.join(", ", properties)
              + ")");
    }
    for (var index : schema.getTextIndexes()) {
      String options = CypherPatterns.schemaOptions(index.getOptions());
      statements.add(
          "CREATE TEXT INDEX "
              + escape(index.getName())
              + " IF NOT EXISTS FOR (n:"
              + escape(index.getLabel())
              + ") ON (n."
              + escape(index.getProperty())
              + ")"
              + (options.isEmpty() ? "" : " " + options));
    }
    for (var index : schema.getPointIndexes()) {
      String options = CypherPatterns.schemaOptions(index.getOptions());
      statements.add(
          "CREATE POINT INDEX "
              + escape(index.getName())
              + " IF NOT EXISTS FOR (n:"
              + escape(index.getLabel())
              + ") ON (n."
              + escape(index.getProperty())
              + ")"
              + (options.isEmpty() ? "" : " " + options));
    }
    for (var index : schema.getFullTextIndexes()) {
      var properties =
          CypherPatterns.qualifyAll("n", CypherPatterns.escapeAll(index.getProperties()));
      var options = CypherPatterns.schemaOptions(index.getOptions());
      statements.add(
          "CREATE FULLTEXT INDEX "
              + escape(index.getName())
              + " IF NOT EXISTS FOR (n"
              + CypherPatterns.labels(index.getLabels(), "|")
              + ") ON EACH ["
              + String.join(", ", properties)
              + "]"
              + (options.isEmpty() ? "" : " " + options));
    }
    if (capabilities.hasVectorIndexes()) {
      for (var index : schema.getVectorIndexes()) {
        var options = CypherPatterns.schemaOptions(index.getOptions());
        statements.add(
            "CREATE VECTOR INDEX "
                + escape(index.getName())
                + " IF NOT EXISTS FOR (n:"
                + escape(index.getLabel())
                + ") ON (n."
                + escape(index.getProperty())
                + ")"
                + (options.isEmpty() ? "" : " " + options));
      }
    }
    return statements;
  }

  private static Set<String> getRelationshipSchemaStatements(
      RelationshipTarget target, Neo4jCapabilities capabilities) {
    RelationshipSchema schema = target.getSchema();
    if (schema == null) {
      return Set.of();
    }
    Set<String> statements = new LinkedHashSet<>();
    String escapedType = escape(target.getType());

    if (capabilities.hasRelationshipTypeConstraints()) {
      Map<String, PropertyType> types = indexPropertyTypes(target.getProperties());
      for (var constraint : schema.getTypeConstraints()) {
        String property = constraint.getProperty();
        statements.add(
            "CREATE CONSTRAINT "
                + escape(constraint.getName())
                + " IF NOT EXISTS FOR ()-[r:"
                + escapedType
                + "]-() REQUIRE r."
                + escape(property)
                + " IS :: "
                + CypherPatterns.propertyType(types.get(property)));
      }
    }

    if (capabilities.hasRelationshipKeyConstraints()) {
      for (var constraint : schema.getKeyConstraints()) {
        var properties =
            CypherPatterns.qualifyAll("r", CypherPatterns.escapeAll(constraint.getProperties()));
        var options = CypherPatterns.schemaOptions(constraint.getOptions());
        statements.add(
            "CREATE CONSTRAINT "
                + escape(constraint.getName())
                + " IF NOT EXISTS FOR ()-[r:"
                + escapedType
                + "]-() REQUIRE ("
                + String.join(", ", properties)
                + ") IS RELATIONSHIP KEY"
                + (options.isEmpty() ? "" : " " + options));
      }
    }

    if (capabilities.hasRelationshipUniqueConstraints()) {
      for (var constraint : schema.getUniqueConstraints()) {
        var properties =
            CypherPatterns.qualifyAll("r", CypherPatterns.escapeAll(constraint.getProperties()));
        var options = CypherPatterns.schemaOptions(constraint.getOptions());
        statements.add(
            "CREATE CONSTRAINT "
                + escape(constraint.getName())
                + " IF NOT EXISTS FOR ()-[r:"
                + escapedType
                + "]-() REQUIRE ("
                + String.join(", ", properties)
                + ") IS UNIQUE"
                + (options.isEmpty() ? "" : " " + options));
      }
    }
    if (capabilities.hasRelationshipExistenceConstraints()) {
      for (var constraint : schema.getExistenceConstraints()) {
        statements.add(
            "CREATE CONSTRAINT "
                + escape(constraint.getName())
                + " IF NOT EXISTS FOR ()-[r:"
                + escapedType
                + "]-() REQUIRE r."
                + escape(constraint.getProperty())
                + " IS NOT NULL");
      }
    }

    for (var index : schema.getRangeIndexes()) {
      var properties =
          CypherPatterns.qualifyAll("r", CypherPatterns.escapeAll(index.getProperties()));
      statements.add(
          "CREATE INDEX "
              + escape(index.getName())
              + " IF NOT EXISTS FOR ()-[r:"
              + escapedType
              + "]-() ON ("
              + String.join(", ", properties)
              + ")");
    }

    for (var index : schema.getTextIndexes()) {
      String options = CypherPatterns.schemaOptions(index.getOptions());
      statements.add(
          "CREATE TEXT INDEX "
              + escape(index.getName())
              + " IF NOT EXISTS FOR ()-[r:"
              + escapedType
              + "]-() ON (r."
              + escape(index.getProperty())
              + ")"
              + (options.isEmpty() ? "" : " " + options));
    }

    for (var index : schema.getPointIndexes()) {
      String options = CypherPatterns.schemaOptions(index.getOptions());
      statements.add(
          "CREATE POINT INDEX "
              + escape(index.getName())
              + " IF NOT EXISTS FOR ()-[r:"
              + escapedType
              + "]-() ON (r."
              + escape(index.getProperty())
              + ")"
              + (options.isEmpty() ? "" : " " + options));
    }

    for (var index : schema.getFullTextIndexes()) {
      String options = CypherPatterns.schemaOptions(index.getOptions());
      var properties =
          CypherPatterns.qualifyAll("r", CypherPatterns.escapeAll(index.getProperties()));
      statements.add(
          "CREATE FULLTEXT INDEX "
              + escape(index.getName())
              + " IF NOT EXISTS FOR ()-[r:"
              + escapedType
              + "]-() ON EACH ["
              + String.join(", ", properties)
              + "]"
              + (options.isEmpty() ? "" : " " + options));
    }

    if (capabilities.hasVectorIndexes()) {
      for (var index : schema.getVectorIndexes()) {
        String options = CypherPatterns.schemaOptions(index.getOptions());
        statements.add(
            "CREATE VECTOR INDEX "
                + escape(index.getName())
                + " IF NOT EXISTS FOR ()-[r:"
                + escapedType
                + "]-() ON (r."
                + escape(index.getProperty())
                + ")"
                + (options.isEmpty() ? "" : " " + options));
      }
    }
    return statements;
  }

  private static NodeTarget resolveRelationshipNode(
      ImportSpecification importSpecification, String reference) {
    return importSpecification.getTargets().getNodes().stream()
        .filter(target -> reference.equals(target.getName()))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format("Could not resolve node target reference %s", reference)));
  }

  private static Map<String, PropertyType> indexPropertyTypes(List<PropertyMapping> target) {
    return target.stream()
        .filter(mapping -> mapping.getTargetPropertyType() != null)
        .collect(toMap(PropertyMapping::getTargetProperty, PropertyMapping::getTargetPropertyType));
  }
}

class CypherPatterns {

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
        .map(entry -> String.format("`%s`: %s", entry.getKey(), schemaOption(entry.getValue())))
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
    return labels.stream().collect(Collectors.joining(String.format("`%s`", separator), ":`", "`"));
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
    String escapedProperty = escape(variable);
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

  public static List<String> escapeAll(Collection<String> properties) {
    return properties.stream().map(CypherPatterns::escape).collect(Collectors.toList());
  }

  public static String escape(String identifier) {
    String id = identifier.trim();
    if (id.charAt(0) == '`' && id.charAt(id.length() - 1) == '`') {
      return id;
    }
    return String.format("`%s`", id);
  }
}
