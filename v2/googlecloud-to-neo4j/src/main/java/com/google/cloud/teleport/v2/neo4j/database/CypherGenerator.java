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

import static com.google.cloud.teleport.v2.neo4j.utils.ModelUtils.filterProperties;

import com.google.cloud.teleport.v2.neo4j.model.enums.EdgeNodesSaveMode;
import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.enums.SaveMode;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

/**
 * Generates cypher based on model metadata.
 *
 * <p>TODO: Needs to be refactored to use DSL.
 */
public class CypherGenerator {
  private static final String CONST_ROW_VARIABLE_NAME = "rows";

  public static String getUnwindCreateCypher(Target target) {
    TargetType targetType = target.getType();
    if (targetType != TargetType.edge && targetType != TargetType.node) {
      throw new RuntimeException("Unhandled target type: " + targetType);
    }

    SaveMode saveMode = target.getSaveMode();
    if (saveMode != SaveMode.merge && saveMode != SaveMode.append) {
      throw new RuntimeException("Unhandled save mode: " + saveMode);
    }

    if (targetType == TargetType.edge) {
      return unwindRelationships(target);
    }

    if (saveMode == SaveMode.merge) {
      return unwindMergeNodes(target);
    }
    return unwindCreateNodes(target);
  }

  private static String unwindCreateNodes(Target target) {
    StringBuilder sb = new StringBuilder();
    sb.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row ");
    sb.append("CREATE (")
        .append(
            getLabelsPropertiesListCypherFragment(
                "n", FragmentType.node, Arrays.asList(RoleType.key, RoleType.property), target))
        .append(")");
    return sb.toString();
  }

  private static String unwindMergeNodes(Target target) {
    StringBuilder sb = new StringBuilder();
    sb.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row ");
    // MERGE clause represents matching properties
    // MERGE (charlie {name: 'Charlie Sheen', age: 10})  A new node with the name 'Charlie
    // Sheen' will be created since not all properties matched the existing 'Charlie Sheen'
    // node.
    sb.append("MERGE (")
        .append(
            getLabelsPropertiesListCypherFragment(
                "n", FragmentType.node, List.of(RoleType.key), target))
        .append(")");
    String nodePropertyMapStr =
        getPropertiesListCypherFragment(FragmentType.node, List.of(RoleType.property), target);
    if (nodePropertyMapStr.length() > 0) {
      sb.append(" SET n+=").append(nodePropertyMapStr);
    }
    return sb.toString();
  }

  private static String unwindRelationships(Target edge) {
    String edgeClause;
    String nodeClause;
    if (edge.getSaveMode() == SaveMode.merge) {
      edgeClause = "MERGE";
      nodeClause = edge.getEdgeNodesMatchMode() == EdgeNodesSaveMode.merge ? "MERGE" : "MATCH";
    } else {
      edgeClause = "CREATE";
      nodeClause = edge.getEdgeNodesMatchMode() == EdgeNodesSaveMode.merge ? "MERGE" : "CREATE";
    }

    StringBuilder query = new StringBuilder();
    query.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row ");
    query
        .append(String.format(" %s (", nodeClause))
        .append(
            getLabelsPropertiesListCypherFragment(
                "source", FragmentType.source, List.of(RoleType.key), edge))
        .append(")");
    query
        .append(String.format(" %s (", nodeClause))
        .append(
            getLabelsPropertiesListCypherFragment(
                "target", FragmentType.target, List.of(RoleType.key), edge))
        .append(")");
    query.append(String.format(" %s (source)", edgeClause));
    query.append("-[").append(getRelationshipTypePropertiesListFragment("rel", edge)).append("]->");
    query.append("(target)");
    String relPropertyMap =
        getPropertiesListCypherFragment(FragmentType.rel, List.of(RoleType.property), edge);
    if (!relPropertyMap.isEmpty()) {
      query.append(" SET rel += ").append(relPropertyMap);
    }
    return query.toString();
  }

  private static String getLabelsPropertiesListCypherFragment(
      String alias, FragmentType entityType, List<RoleType> roleTypes, Target target) {
    StringBuilder sb = new StringBuilder();
    List<String> labels =
        ModelUtils.getStaticOrDynamicLabels(CONST_ROW_VARIABLE_NAME, entityType, target);
    String propertiesKeyListStr = getPropertiesListCypherFragment(entityType, roleTypes, target);
    // Labels
    if (labels.size() > 0) {
      sb.append(alias);
      for (String label : labels) {
        sb.append(":").append(ModelUtils.makeSpaceSafeValidNeo4jIdentifier(label.trim()));
      }
    } else if (StringUtils.isNotEmpty(target.getName())) {
      sb.append(alias);
      sb.append(":").append(ModelUtils.makeSpaceSafeValidNeo4jIdentifier(target.getName()));
    } else {
      sb.append(alias);
    }
    if (StringUtils.isNotEmpty(propertiesKeyListStr)) {
      sb.append(" ").append(propertiesKeyListStr);
    }
    return sb.toString();
  }

  private static String getPropertiesListCypherFragment(
      FragmentType entityType, List<RoleType> roleTypes, Target target) {
    StringBuilder sb = new StringBuilder();
    int targetColCount = 0;
    for (Mapping m : target.getMappings()) {
      if (m.getFragmentType() == entityType) {
        if (roleTypes.contains(m.getRole())) {
          if (targetColCount > 0) {
            sb.append(",");
          }
          if (StringUtils.isNotEmpty(m.getConstant())) {
            sb.append(ModelUtils.makeSpaceSafeValidNeo4jIdentifier(m.getName()))
                .append(": \"")
                .append(m.getConstant())
                .append("\"");
          } else {
            sb.append(ModelUtils.makeSpaceSafeValidNeo4jIdentifier(m.getName()))
                .append(": row.")
                .append(m.getField());
          }
          targetColCount++;
        }
      }
    }
    if (sb.length() > 0) {
      return "{" + sb + "}";
    }
    return "";
  }

  /**
   * Generates the Cypher schema statements for the given target.
   *
   * @param target a target validated by {@link
   *     com.google.cloud.teleport.v2.neo4j.model.InputValidator} and transformed by {@link
   *     com.google.cloud.teleport.v2.neo4j.model.InputRefactoring}
   * @return a list of Cypher schema statements
   */
  public static Set<String> getIndexAndConstraintsCypherStatements(
      Target target, Neo4jCapabilities capabilities) {
    TargetType type = target.getType();
    switch (type) {
      case node:
        return getNodeIndexAndConstraintsCypherStatements(target, capabilities);
      case edge:
        return getRelationshipIndexAndConstraintsCypherStatements(target, capabilities);
      default:
        return Collections.emptySet();
    }
  }

  private static Set<String> getNodeIndexAndConstraintsCypherStatements(
      Target target, Neo4jCapabilities capabilities) {
    List<String> labels = ModelUtils.getStaticLabels(FragmentType.node, target);
    Set<String> keyProperties =
        filterProperties(
            target,
            (mapping) ->
                mapping.getFragmentType() == FragmentType.node
                    && mapping.getRole() == RoleType.key);
    Set<String> uniqueProperties =
        filterProperties(
            target,
            (mapping) -> isEntityProperty(mapping, FragmentType.node) && mapping.isUnique());
    Set<String> mandatoryProperties =
        filterProperties(
            target,
            (mapping) -> isEntityProperty(mapping, FragmentType.node) && mapping.isMandatory());
    Set<String> indexedProperties =
        filterProperties(
            target,
            (mapping) -> isEntityProperty(mapping, FragmentType.node) && mapping.isIndexed());

    Set<String> cyphers = new LinkedHashSet<>();

    if (capabilities.hasNodeKeyConstraints()) {
      cyphers.addAll(getEntityKeyConstraintStatements(labels, keyProperties));
    }

    if (capabilities.hasNodeUniqueConstraints()) {
      for (String uniqueProperty : uniqueProperties) {
        cyphers.add(
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:"
                + StringUtils.join(ModelUtils.makeSpaceSafeValidNeo4jIdentifiers(labels), ":")
                + ") REQUIRE n."
                + ModelUtils.makeSpaceSafeValidNeo4jIdentifier(uniqueProperty)
                + " IS UNIQUE");
      }
    }

    if (capabilities.hasNodeExistenceConstraints()) {
      for (String mandatoryProperty : mandatoryProperties) {
        cyphers.add(
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:"
                + StringUtils.join(ModelUtils.makeSpaceSafeValidNeo4jIdentifiers(labels), ":")
                + ") REQUIRE n."
                + ModelUtils.makeSpaceSafeValidNeo4jIdentifier(mandatoryProperty)
                + " IS NOT NULL");
      }
    }

    for (String indexedProperty : indexedProperties) {
      cyphers.add(
          "CREATE INDEX IF NOT EXISTS FOR (t:"
              + StringUtils.join(ModelUtils.makeSpaceSafeValidNeo4jIdentifiers(labels), ":")
              + ") ON (t."
              + ModelUtils.makeSpaceSafeValidNeo4jIdentifier(indexedProperty)
              + ")");
    }

    return cyphers;
  }

  private static Set<String> getRelationshipIndexAndConstraintsCypherStatements(
      Target target, Neo4jCapabilities capabilities) {
    Set<String> cyphers = new LinkedHashSet<>();
    if (capabilities.hasNodeKeyConstraints()
        && target.getEdgeNodesMatchMode() == EdgeNodesSaveMode.merge) {
      cyphers.addAll(
          getEntityKeyConstraintStatements(
              ModelUtils.getStaticLabels(FragmentType.source, target),
              filterProperties(
                  target,
                  (mapping) ->
                      mapping.getFragmentType() == FragmentType.source
                          && mapping.getRole() == RoleType.key)));
      cyphers.addAll(
          getEntityKeyConstraintStatements(
              ModelUtils.getStaticLabels(FragmentType.target, target),
              filterProperties(
                  target,
                  (mapping) ->
                      mapping.getFragmentType() == FragmentType.target
                          && mapping.getRole() == RoleType.key)));
    }

    String type = ModelUtils.getStaticType(target);
    Set<String> keyProperties =
        filterProperties(
            target,
            (mapping) ->
                mapping.getFragmentType() == FragmentType.rel && mapping.getRole() == RoleType.key);
    Set<String> uniqueProperties =
        filterProperties(
            target, (mapping) -> isEntityProperty(mapping, FragmentType.rel) && mapping.isUnique());
    Set<String> mandatoryProperties =
        filterProperties(
            target,
            (mapping) -> isEntityProperty(mapping, FragmentType.rel) && mapping.isMandatory());
    Set<String> indexedProperties =
        filterProperties(
            target,
            (mapping) -> isEntityProperty(mapping, FragmentType.rel) && mapping.isIndexed());

    String escapedType = ModelUtils.makeSpaceSafeValidNeo4jIdentifier(type);
    if (capabilities.hasRelationshipKeyConstraints()) {
      for (String relKeyProperty : keyProperties) {
        cyphers.add(
            "CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:"
                + escapedType
                + "]-() REQUIRE r."
                + ModelUtils.makeSpaceSafeValidNeo4jIdentifier(relKeyProperty)
                + " IS RELATIONSHIP KEY");
      }
    }

    if (capabilities.hasRelationshipUniqueConstraints()) {
      for (String uniqueProperty : uniqueProperties) {
        cyphers.add(
            "CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:"
                + escapedType
                + "]-() REQUIRE r."
                + ModelUtils.makeSpaceSafeValidNeo4jIdentifier(uniqueProperty)
                + " IS UNIQUE");
      }
    }

    if (capabilities.hasRelationshipExistenceConstraints()) {
      for (String mandatoryProperty : mandatoryProperties) {
        cyphers.add(
            "CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:"
                + escapedType
                + "]-() REQUIRE r."
                + ModelUtils.makeSpaceSafeValidNeo4jIdentifier(mandatoryProperty)
                + " IS NOT NULL");
      }
    }

    for (String indexedProperty : indexedProperties) {
      cyphers.add(
          "CREATE INDEX IF NOT EXISTS FOR ()-[r:"
              + escapedType
              + "]-() ON (r."
              + ModelUtils.makeSpaceSafeValidNeo4jIdentifier(indexedProperty)
              + ")");
    }

    return cyphers;
  }

  private static List<String> getEntityKeyConstraintStatements(
      List<String> labels, Set<String> nodeKeyProperties) {
    List<String> statements = new ArrayList<>(labels.size());
    for (String label : labels) {
      String escapedLabel = ModelUtils.makeSpaceSafeValidNeo4jIdentifier(label);
      // TODO: distinguish keys vs key mapping
      for (String nodeKeyProperty : nodeKeyProperties) {
        String escapedProperty = ModelUtils.makeSpaceSafeValidNeo4jIdentifier(nodeKeyProperty);
        statements.add(
            String.format(
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:%s) REQUIRE n.%s IS NODE KEY",
                escapedLabel, escapedProperty));
      }
    }
    return statements;
  }

  private static String getRelationshipTypePropertiesListFragment(String prefix, Target target) {
    StringBuilder sb = new StringBuilder();
    List<String> relType =
        ModelUtils.getStaticOrDynamicRelationshipType(CONST_ROW_VARIABLE_NAME, target);
    sb.append(prefix).append(":").append(StringUtils.join(relType, ":"));
    String properties =
        getPropertiesListCypherFragment(FragmentType.rel, List.of(RoleType.key), target);
    if (!properties.isEmpty()) {
      sb.append(" ").append(properties);
    }
    return sb.toString();
  }

  private static boolean isEntityProperty(Mapping mapping, FragmentType entityType) {
    return mapping.getFragmentType() == entityType && mapping.getRole() == RoleType.property;
  }
}
