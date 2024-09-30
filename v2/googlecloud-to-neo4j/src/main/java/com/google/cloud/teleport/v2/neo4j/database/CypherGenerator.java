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

import static com.google.cloud.teleport.v2.neo4j.database.CypherPatterns.sanitize;
import static java.util.stream.Collectors.toMap;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        + sanitize(relationship.getType())
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
                + sanitize(constraint.getName())
                + " IF NOT EXISTS FOR (n:"
                + sanitize(constraint.getLabel())
                + ") REQUIRE n."
                + sanitize(property)
                + " IS :: "
                + CypherPatterns.propertyType(types.get(property)));
      }
    }
    if (capabilities.hasNodeKeyConstraints()) {
      for (var constraint : schema.getKeyConstraints()) {
        var properties =
            CypherPatterns.qualifyAll("n", CypherPatterns.sanitizeAll(constraint.getProperties()));
        var options = CypherPatterns.schemaOptions(constraint.getOptions());
        statements.add(
            "CREATE CONSTRAINT "
                + sanitize(constraint.getName())
                + " IF NOT EXISTS FOR (n:"
                + sanitize(constraint.getLabel())
                + ") REQUIRE ("
                + String.join(", ", properties)
                + ") IS NODE KEY"
                + (options.isEmpty() ? "" : " " + options));
      }
    }
    if (capabilities.hasNodeUniqueConstraints()) {
      for (var constraint : schema.getUniqueConstraints()) {
        var properties =
            CypherPatterns.qualifyAll("n", CypherPatterns.sanitizeAll(constraint.getProperties()));
        var options = CypherPatterns.schemaOptions(constraint.getOptions());
        statements.add(
            "CREATE CONSTRAINT "
                + sanitize(constraint.getName())
                + " IF NOT EXISTS FOR (n:"
                + sanitize(constraint.getLabel())
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
                + sanitize(constraint.getName())
                + " IF NOT EXISTS FOR (n:"
                + sanitize(constraint.getLabel())
                + ") REQUIRE n."
                + sanitize(constraint.getProperty())
                + " IS NOT NULL");
      }
    }
    for (var index : schema.getRangeIndexes()) {
      var properties =
          CypherPatterns.qualifyAll("n", CypherPatterns.sanitizeAll(index.getProperties()));
      statements.add(
          "CREATE INDEX "
              + sanitize(index.getName())
              + " IF NOT EXISTS FOR (n:"
              + sanitize(index.getLabel())
              + ") ON ("
              + String.join(", ", properties)
              + ")");
    }
    for (var index : schema.getTextIndexes()) {
      String options = CypherPatterns.schemaOptions(index.getOptions());
      statements.add(
          "CREATE TEXT INDEX "
              + sanitize(index.getName())
              + " IF NOT EXISTS FOR (n:"
              + sanitize(index.getLabel())
              + ") ON (n."
              + sanitize(index.getProperty())
              + ")"
              + (options.isEmpty() ? "" : " " + options));
    }
    for (var index : schema.getPointIndexes()) {
      String options = CypherPatterns.schemaOptions(index.getOptions());
      statements.add(
          "CREATE POINT INDEX "
              + sanitize(index.getName())
              + " IF NOT EXISTS FOR (n:"
              + sanitize(index.getLabel())
              + ") ON (n."
              + sanitize(index.getProperty())
              + ")"
              + (options.isEmpty() ? "" : " " + options));
    }
    for (var index : schema.getFullTextIndexes()) {
      var properties =
          CypherPatterns.qualifyAll("n", CypherPatterns.sanitizeAll(index.getProperties()));
      var options = CypherPatterns.schemaOptions(index.getOptions());
      statements.add(
          "CREATE FULLTEXT INDEX "
              + sanitize(index.getName())
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
                + sanitize(index.getName())
                + " IF NOT EXISTS FOR (n:"
                + sanitize(index.getLabel())
                + ") ON (n."
                + sanitize(index.getProperty())
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
    String escapedType = sanitize(target.getType());

    if (capabilities.hasRelationshipTypeConstraints()) {
      Map<String, PropertyType> types = indexPropertyTypes(target.getProperties());
      for (var constraint : schema.getTypeConstraints()) {
        String property = constraint.getProperty();
        statements.add(
            "CREATE CONSTRAINT "
                + sanitize(constraint.getName())
                + " IF NOT EXISTS FOR ()-[r:"
                + escapedType
                + "]-() REQUIRE r."
                + sanitize(property)
                + " IS :: "
                + CypherPatterns.propertyType(types.get(property)));
      }
    }

    if (capabilities.hasRelationshipKeyConstraints()) {
      for (var constraint : schema.getKeyConstraints()) {
        var properties =
            CypherPatterns.qualifyAll("r", CypherPatterns.sanitizeAll(constraint.getProperties()));
        var options = CypherPatterns.schemaOptions(constraint.getOptions());
        statements.add(
            "CREATE CONSTRAINT "
                + sanitize(constraint.getName())
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
            CypherPatterns.qualifyAll("r", CypherPatterns.sanitizeAll(constraint.getProperties()));
        var options = CypherPatterns.schemaOptions(constraint.getOptions());
        statements.add(
            "CREATE CONSTRAINT "
                + sanitize(constraint.getName())
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
                + sanitize(constraint.getName())
                + " IF NOT EXISTS FOR ()-[r:"
                + escapedType
                + "]-() REQUIRE r."
                + sanitize(constraint.getProperty())
                + " IS NOT NULL");
      }
    }

    for (var index : schema.getRangeIndexes()) {
      var properties =
          CypherPatterns.qualifyAll("r", CypherPatterns.sanitizeAll(index.getProperties()));
      statements.add(
          "CREATE INDEX "
              + sanitize(index.getName())
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
              + sanitize(index.getName())
              + " IF NOT EXISTS FOR ()-[r:"
              + escapedType
              + "]-() ON (r."
              + sanitize(index.getProperty())
              + ")"
              + (options.isEmpty() ? "" : " " + options));
    }

    for (var index : schema.getPointIndexes()) {
      String options = CypherPatterns.schemaOptions(index.getOptions());
      statements.add(
          "CREATE POINT INDEX "
              + sanitize(index.getName())
              + " IF NOT EXISTS FOR ()-[r:"
              + escapedType
              + "]-() ON (r."
              + sanitize(index.getProperty())
              + ")"
              + (options.isEmpty() ? "" : " " + options));
    }

    for (var index : schema.getFullTextIndexes()) {
      String options = CypherPatterns.schemaOptions(index.getOptions());
      var properties =
          CypherPatterns.qualifyAll("r", CypherPatterns.sanitizeAll(index.getProperties()));
      statements.add(
          "CREATE FULLTEXT INDEX "
              + sanitize(index.getName())
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
                + sanitize(index.getName())
                + " IF NOT EXISTS FOR ()-[r:"
                + escapedType
                + "]-() ON (r."
                + sanitize(index.getProperty())
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
