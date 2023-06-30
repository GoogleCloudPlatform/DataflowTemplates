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

import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.enums.SaveMode;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.Config;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates cypher based on model metadata.
 *
 * <p>TODO: Needs to be refactored to use DSL.
 */
public class CypherGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(CypherGenerator.class);
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
      if (saveMode == SaveMode.merge) {
        return unwindMergeRelationships(target);
      }
      return unwindCreateRelationships(target);
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
                "n",
                false,
                FragmentType.node,
                Arrays.asList(RoleType.key, RoleType.property),
                    target))
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
                "n", false, FragmentType.node, List.of(RoleType.key), target))
        .append(")");
    String nodePropertyMapStr =
        getPropertiesListCypherFragment(
            FragmentType.node, false, List.of(RoleType.property), target);
    if (nodePropertyMapStr.length() > 0) {
      sb.append(" SET n+=").append(nodePropertyMapStr);
    }
    return sb.toString();
  }

  private static String unwindCreateRelationships(Target target) {
    StringBuilder sb = new StringBuilder();
    sb.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row CREATE ");
    sb.append("(")
        .append(
            getLabelsPropertiesListCypherFragment(
                "source",
                false,
                FragmentType.source,
                Arrays.asList(RoleType.key, RoleType.property),
                    target))
        .append(")");
    sb.append(" -[")
        .append(getRelationshipTypePropertiesListFragment("rel", false, target))
        .append("]-> ");
    sb.append("(")
        .append(
            getLabelsPropertiesListCypherFragment(
                "target",
                false,
                FragmentType.target,
                Arrays.asList(RoleType.key, RoleType.property),
                    target))
        .append(")");
    String relPropertyMap =
        getPropertiesListCypherFragment(
            FragmentType.rel, false, List.of(RoleType.property), target);
    if (relPropertyMap.length() > 0) {
      sb.append(" SET rel += ").append(relPropertyMap);
    }
    return sb.toString();
  }

  private static String unwindMergeRelationships(Target target) {
    StringBuilder sb = new StringBuilder();
    sb.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row ");
    // MERGE (variable1:Label1 {nodeProperties1})-[:REL_TYPE]->
    // (variable2:Label2 {nodeProperties2})
    // MATCH before MERGE
    sb.append(" MATCH (")
        .append(
            getLabelsPropertiesListCypherFragment(
                "source", true, FragmentType.source, List.of(RoleType.key), target))
        .append(")");
    sb.append(" MATCH (")
        .append(
            getLabelsPropertiesListCypherFragment(
                "target", true, FragmentType.target, List.of(RoleType.key), target))
        .append(")");
    sb.append(" MERGE (source)");
    sb.append("-[")
        .append(getRelationshipTypePropertiesListFragment("rel", true, target))
        .append("]->");
    sb.append("(target)");
    String relPropertyMap =
        getPropertiesListCypherFragment(
            FragmentType.rel, false, List.of(RoleType.property), target);
    if (relPropertyMap.length() > 0) {
      sb.append(" SET rel += ").append(relPropertyMap);
    }
    return sb.toString();
  }

  public static String getLabelsPropertiesListCypherFragment(
      String alias,
      boolean onlyIndexedProperties,
      FragmentType entityType,
      List<RoleType> roleTypes,
      Target target) {
    StringBuilder sb = new StringBuilder();
    List<String> labels =
        ModelUtils.getStaticOrDynamicLabels(CONST_ROW_VARIABLE_NAME, entityType, target);
    String propertiesKeyListStr =
        getPropertiesListCypherFragment(entityType, onlyIndexedProperties, roleTypes, target);
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

  public static String getPropertiesListCypherFragment(
      FragmentType entityType,
      boolean onlyIndexedProperties,
      List<RoleType> roleTypes,
      Target target) {
    StringBuilder sb = new StringBuilder();
    int targetColCount = 0;
    for (Mapping m : target.getMappings()) {
      if (m.getFragmentType() == entityType) {
        if (roleTypes.contains(m.getRole()) && (!onlyIndexedProperties || m.isIndexed())) {
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

  public static List<String> getIndexAndConstraintsCypherStatements(
      TargetType type, Config config, Target target) {
    switch (type) {
      case node:
        return getNodeIndexAndConstraintsCypherStatements(config, target);
      case edge:
        return getRelationshipIndexAndConstraintsCypherStatements(config, target);
      default:
        throw new IllegalArgumentException(String.format("unexpected target type: %s", type));
    }
  }

  private static List<String> getNodeIndexAndConstraintsCypherStatements(
      Config config, Target target) {

    List<String> cyphers = new ArrayList<>();
    // Model node creation statement
    //  "UNWIND $rows AS row CREATE(c:Customer { id : row.id, name: row.name, firstName:
    // row.firstName })
    // derive labels
    List<String> labels = ModelUtils.getStaticLabels(target);
    List<String> indexedProperties =
        ModelUtils.getIndexedProperties(config.getIndexAllProperties(), FragmentType.node, target);
    List<String> uniqueProperties = ModelUtils.getUniqueProperties(FragmentType.node, target);
    List<String> mandatoryProperties = ModelUtils.getRequiredProperties(FragmentType.node, target);
    List<String> nodeKeyProperties = ModelUtils.getEntityKeyProperties(FragmentType.node, target);

    for (String uniqueProperty : uniqueProperties) {
      cyphers.add(
          "CREATE CONSTRAINT IF NOT EXISTS FOR (n:"
              + StringUtils.join(ModelUtils.makeSpaceSafeValidNeo4jIdentifiers(labels), ":")
              + ") REQUIRE n."
              + ModelUtils.makeSpaceSafeValidNeo4jIdentifier(uniqueProperty)
              + " IS UNIQUE");
    }
    for (String mandatoryProperty : mandatoryProperties) {
      cyphers.add(
          "CREATE CONSTRAINT IF NOT EXISTS FOR (n:"
              + StringUtils.join(ModelUtils.makeSpaceSafeValidNeo4jIdentifiers(labels), ":")
              + ") REQUIRE n."
              + ModelUtils.makeSpaceSafeValidNeo4jIdentifier(mandatoryProperty)
              + " IS NOT NULL");
    }
    for (String nodeKeyProperty : nodeKeyProperties) {
      cyphers.add(
          "CREATE CONSTRAINT IF NOT EXISTS FOR (n:"
              + StringUtils.join(ModelUtils.makeSpaceSafeValidNeo4jIdentifiers(labels), ":")
              + ") REQUIRE n."
              + ModelUtils.makeSpaceSafeValidNeo4jIdentifier(nodeKeyProperty)
              + " IS NODE KEY");
    }
    // constraints must be created last
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

  // TODO: no-op if < 5.7 || not EE for some or all
  private static List<String> getRelationshipIndexAndConstraintsCypherStatements(
      Config config, Target target) {

    List<String> cyphers = new ArrayList<>();
    // Model node creation statement
    //  "UNWIND $rows AS row CREATE(c:Customer { id : row.id, name: row.name, firstName:
    // row.firstName })
    // derive labels
    String type = ModelUtils.getStaticType(target);
    List<String> indexedProperties =
        ModelUtils.getIndexedProperties(config.getIndexAllProperties(), FragmentType.rel, target);
    List<String> uniqueProperties = ModelUtils.getUniqueProperties(FragmentType.rel, target);
    List<String> mandatoryProperties = ModelUtils.getRequiredProperties(FragmentType.rel, target);
    List<String> relKeyProperties = ModelUtils.getEntityKeyProperties(FragmentType.rel, target);

    String escapedType = ModelUtils.makeSpaceSafeValidNeo4jIdentifier(type);
    for (String uniqueProperty : uniqueProperties) {
      cyphers.add(
          "CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:"
              + escapedType
              + "]-() REQUIRE r."
              + ModelUtils.makeSpaceSafeValidNeo4jIdentifier(uniqueProperty)
              + " IS UNIQUE");
    }
    for (String mandatoryProperty : mandatoryProperties) {
      cyphers.add(
          "CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:"
              + escapedType
              + "]-() REQUIRE r."
              + ModelUtils.makeSpaceSafeValidNeo4jIdentifier(mandatoryProperty)
              + " IS NOT NULL");
    }
    for (String relKeyProperty : relKeyProperties) {
      cyphers.add(
          "CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:"
              + escapedType
              + "]-() REQUIRE r."
              + ModelUtils.makeSpaceSafeValidNeo4jIdentifier(relKeyProperty)
              + " IS RELATIONSHIP KEY");
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

  public static String getRelationshipTypePropertiesListFragment(
      String prefix, boolean onlyIndexedProperties, Target target) {
    StringBuilder sb = new StringBuilder();
    List<String> relType =
        ModelUtils.getStaticOrDynamicRelationshipType(CONST_ROW_VARIABLE_NAME, target);
    sb.append(prefix).append(":").append(StringUtils.join(relType, ":"));
    String properties =
        getPropertiesListCypherFragment(
            FragmentType.rel, onlyIndexedProperties, List.of(RoleType.key), target);
    if (!properties.isEmpty()) {
      sb.append(" ").append(properties);
    }
    return sb.toString();
  }
}
