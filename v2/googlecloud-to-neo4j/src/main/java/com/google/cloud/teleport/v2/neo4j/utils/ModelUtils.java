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
package com.google.cloud.teleport.v2.neo4j.utils;

import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.Aggregation;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.model.job.Transform;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility functions for Beam rows and schema. */
public class ModelUtils {
  public static final String DEFAULT_STAR_QUERY = "SELECT * FROM PCOLLECTION";
  private static final String neoIdentifierDisAllowedCharactersRegex = "[^a-zA-Z0-9_]";
  private static final String neoIdentifierDisAllowedCharactersRegexIncSpace = "[^a-zA-Z0-9_ ]";
  private static final String nonAlphaCharsRegex = "[^a-zA-Z]";
  private static final Pattern variablePattern = Pattern.compile("(\\$([a-zA-Z0-9_]+))");
  private static final Logger LOG = LoggerFactory.getLogger(ModelUtils.class);

  public static String getRelationshipKeyField(Target target, FragmentType fragmentType) {
    return getFirstField(target, fragmentType, ImmutableList.of(RoleType.key));
  }

  public static String getFirstField(
      Target target, FragmentType fragmentType, List<RoleType> roleTypes) {
    List<String> fields = getFields(fragmentType, roleTypes, target);
    if (fields.size() > 0) {
      return fields.get(0);
    }
    return "";
  }

  public static String getFirstFieldOrConstant(
      Target target, FragmentType fragmentType, List<RoleType> roleTypes) {
    List<String> fieldsOrConstants = getFieldOrConstants(fragmentType, roleTypes, target);
    if (fieldsOrConstants.size() > 0) {
      return fieldsOrConstants.get(0);
    }
    return "";
  }

  public static boolean targetsHaveTransforms(JobSpec jobSpec, Source source) {
    for (Target target : jobSpec.getTargets()) {
      if (target.isActive()) {
        if (target.getSource().equals(source.getName())) {
          boolean targetRequiresRequery = ModelUtils.targetHasTransforms(target);
          if (targetRequiresRequery) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public static boolean targetHasTransforms(Target target) {
    boolean requiresRequery = false;
    if (target.getTransform() != null) {
      if (target.getTransform().isGroup()
          || target.getTransform().getAggregations().size() > 0
          || StringUtils.isNotEmpty(target.getTransform().getOrderBy())
          || StringUtils.isNotEmpty(target.getTransform().getWhere())) {
        return true;
      }
    }
    return requiresRequery;
  }

  public static Set<String> getBeamFieldSet(Schema schema) {
    return new HashSet<>(schema.getFieldNames());
  }

  public static String getTargetSql(
      Set<String> fieldNameMap, Target target, boolean generateSqlSort) {
    return getTargetSql(fieldNameMap, target, generateSqlSort, null);
  }

  public static String getTargetSql(
      Set<String> fieldNameMap, Target target, boolean generateSqlSort, String baseSql) {
    StringBuilder sb = new StringBuilder();

    String orderByClause = "";
    if (target.getType() == TargetType.edge) {
      String sortField = getRelationshipKeyField(target, FragmentType.target);
      if (StringUtils.isNotBlank(sortField)) {
        orderByClause = " ORDER BY " + sortField + " ASC";
      } else if (StringUtils.isNotBlank(target.getTransform().getOrderBy())) {
        orderByClause = " ORDER BY " + target.getTransform().getOrderBy();
      }
    } else {
      if (StringUtils.isNotBlank(target.getTransform().getOrderBy())) {
        orderByClause = " ORDER BY " + target.getTransform().getOrderBy();
      }
    }

    if (target.getTransform() != null) {
      List<String> fieldList = new ArrayList<>();
      /////////////////////////////////
      // Grouping transform
      Transform query = target.getTransform();
      if (query.isGroup() || query.getAggregations().size() > 0) {
        for (int i = 0; i < target.getMappings().size(); i++) {
          Mapping mapping = target.getMappings().get(i);
          if (StringUtils.isNotBlank(mapping.getField())) {
            if (fieldNameMap.contains(mapping.getField())) {
              fieldList.add(mapping.getField());
            }
          }
        }
        if (fieldList.size() == 0) {
          throw new RuntimeException(
              "Could not find mapped fields for target: "
                  + target.getName()
                  + ". Please verify that target fields exist in source query.");
        }
        sb.append("SELECT ").append(StringUtils.join(fieldList, ","));
        if (query.getAggregations().size() > 0) {
          for (Aggregation agg : query.getAggregations()) {
            sb.append(",").append(agg.getExpression()).append(" ").append(agg.getField());
          }
        }
        sb.append(" FROM PCOLLECTION");
        if (StringUtils.isNotBlank(query.getWhere())) {
          sb.append(" WHERE ").append(query.getWhere());
        }
        sb.append(" GROUP BY ").append(StringUtils.join(fieldList, ","));

        if (StringUtils.isNotEmpty(orderByClause) && generateSqlSort) {
          LOG.info("Order by clause: " + orderByClause);
          sb.append(orderByClause);
          //  ORDER BY without a LIMIT is not supported!
          if (query.getLimit() > -1) {
            sb.append(" LIMIT ").append(query.getLimit());
          }
        } else {
          if (query.getLimit() > -1) {
            sb.append(" LIMIT ").append(query.getLimit());
          }
        }
      }
    }

    // If edge/relationship, sort by destination nodeId to reduce locking
    String innerSql;
    if (sb.length() == 0 && generateSqlSort) {
      innerSql = DEFAULT_STAR_QUERY + orderByClause;
    } else if (sb.length() == 0) {
      innerSql = DEFAULT_STAR_QUERY;
    } else {
      innerSql = sb.toString();
    }
    if (StringUtils.isNotEmpty(baseSql)) {
      return innerSql.replace(" PCOLLECTION", " (" + baseSql + ")");
    } else {
      return innerSql;
    }
  }

  public static String makeValidNeo4jIdentifier(String proposedIdString) {
    if (isQuoted(proposedIdString)) {
      return proposedIdString;
    }
    String finalIdString =
        proposedIdString.trim().replaceAll(neoIdentifierDisAllowedCharactersRegexIncSpace, "_");
    if (finalIdString.substring(0, 1).matches(nonAlphaCharsRegex)) {
      finalIdString = "_" + finalIdString;
    }
    return finalIdString;
  }

  public static String makeSpaceSafeValidNeo4jIdentifier(String proposedIdString) {
    proposedIdString = makeValidNeo4jIdentifier(proposedIdString);
    return backTickedExpressionWithSpaces(proposedIdString);
  }

  public static List<String> makeSpaceSafeValidNeo4jIdentifiers(List<String> proposedIds) {
    List<String> safeList = new ArrayList<>();
    for (String proposed : proposedIds) {
      safeList.add(makeSpaceSafeValidNeo4jIdentifier(proposed));
    }
    return safeList;
  }

  private static String backTickedExpressionWithSpaces(String expression) {
    if (expression.indexOf(" ") == -1) {
      return expression;
    }
    String trExpression = expression.trim();
    if (trExpression.startsWith("`")
        || trExpression.startsWith("\"")
        || trExpression.startsWith("'")) {
      // already starts with quotes...
      // NOTE_TODO: strip existing quotes, replace with double quotes
    } else {
      trExpression = "`" + trExpression.trim() + "`";
    }
    return trExpression;
  }

  public static boolean isQuoted(String expression) {
    String trExpression = expression.trim();
    return (trExpression.startsWith("\"") || trExpression.startsWith("'"))
        && (trExpression.endsWith("\"") || trExpression.endsWith("'"));
  }

  // Make relationships idenfifiers upper case, no spaces
  public static String makeValidNeo4jRelationshipTypeIdentifier(String proposedTypeIdString) {
    String finalIdString =
        proposedTypeIdString
            .replaceAll(neoIdentifierDisAllowedCharactersRegex, "_")
            .toUpperCase()
            .trim();
    if (!finalIdString.substring(0, 1).matches(nonAlphaCharsRegex)) {
      finalIdString = "N" + finalIdString;
    }
    return finalIdString;
  }

  public static List<String> getStaticOrDynamicRelationshipType(
      String dynamicRowPrefix, Target target) {
    List<String> relationships = new ArrayList<>();
    for (Mapping m : target.getMappings()) {
      if (m.getFragmentType() == FragmentType.rel) {
        if (m.getRole() == RoleType.type) {
          if (StringUtils.isNotEmpty(m.getConstant())) {
            relationships.add(m.getConstant());
          } else {
            // TODO: handle dynamic labels
            relationships.add(dynamicRowPrefix + "." + m.getField());
          }
        }
      }
    }
    if (relationships.isEmpty()) {
      // if relationship labels are not defined, use target name
      relationships.add(ModelUtils.makeValidNeo4jRelationshipTypeIdentifier(target.getName()));
    }
    return relationships;
  }

  public static List<String> getStaticLabels(FragmentType entityType, Target target) {
    List<String> labels = new ArrayList<>();
    for (Mapping m : target.getMappings()) {
      if (m.getFragmentType() == entityType) {
        if (m.getLabels().size() > 0) {
          labels.addAll(m.getLabels());
        } else if (m.getRole() == RoleType.label) {
          if (StringUtils.isNotEmpty(m.getConstant())) {
            labels.add(m.getConstant());
          } else {
            // we cannot index on dynamic labels.  These would need to happen with a pre-transform
            // action
            // dynamic labels not handled here
            // labels.add(prefix+"."+m.field);
          }
        }
      }
    }
    return labels;
  }

  public static List<String> getStaticOrDynamicLabels(
      String dynamicRowPrefix, FragmentType entityType, Target target) {
    List<String> labels = new ArrayList<>();
    for (Mapping m : target.getMappings()) {
      if (m.getFragmentType() == entityType) {
        if (m.getLabels().size() > 0) {
          labels.addAll(m.getLabels());
        } else if (m.getRole() == RoleType.label) {
          if (StringUtils.isNotEmpty(m.getConstant())) {
            labels.add(m.getConstant());
          } else {
            labels.add(dynamicRowPrefix + "." + m.getField());
          }
        }
      }
    }
    return labels;
  }

  public static List<String> getFields(
      FragmentType entityType, List<RoleType> roleTypes, Target target) {
    List<String> fieldNames = new ArrayList<>();
    for (Mapping m : target.getMappings()) {
      if (m.getFragmentType() == entityType) {
        if (roleTypes.contains(m.getRole())) {
          fieldNames.add(m.getField());
        }
      }
    }
    return fieldNames;
  }

  public static List<String> getFieldOrConstants(
      FragmentType entityType, List<RoleType> roleTypes, Target target) {
    List<String> fieldOrConstants = new ArrayList<>();
    for (Mapping m : target.getMappings()) {
      if (m.getFragmentType() == entityType) {
        if (roleTypes.contains(m.getRole())) {
          if (StringUtils.isNotBlank(m.getConstant())) {
            fieldOrConstants.add(m.getConstant());
          } else if (StringUtils.isNotBlank(m.getField())) {
            fieldOrConstants.add(m.getField());
          }
        }
      }
    }
    return fieldOrConstants;
  }

  public static String replaceVariableTokens(String text, HashMap<String, String> replacements) {
    Matcher matcher = variablePattern.matcher(text);
    // populate the replacements map ...
    StringBuilder builder = new StringBuilder();
    int i = 0;
    while (matcher.find()) {
      LOG.info("Matcher group: " + matcher.group(1));
      String replacement = replacements.get(matcher.group(2));
      builder.append(text, i, matcher.start());
      if (replacement == null) {
        builder.append(matcher.group(1));
      } else {
        builder.append(replacement);
      }
      i = matcher.end();
    }
    builder.append(text.substring(i));
    String replacedText = builder.toString();
    LOG.info("Before: " + text + ", after: " + replacedText);
    return replacedText;
  }

  public static List<String> getIndexedProperties(
      boolean indexAllProperties, FragmentType entityType, Target target) {
    List<String> indexedProperties = new ArrayList<>();
    for (Mapping m : target.getMappings()) {

      if (m.getFragmentType() == entityType) {
        if (m.getRole() == RoleType.key || m.isIndexed() || indexAllProperties) {
          indexedProperties.add(m.getName());
        }
      }
    }
    return indexedProperties;
  }

  public static List<String> getUniqueProperties(FragmentType entityType, Target target) {
    List<String> uniqueProperties = new ArrayList<>();
    for (Mapping m : target.getMappings()) {

      if (m.getFragmentType() == entityType) {
        if (m.isUnique() || m.getRole() == RoleType.key) {
          uniqueProperties.add(m.getName());
        }
      }
    }
    return uniqueProperties;
  }

  public static List<String> getRequiredProperties(FragmentType entityType, Target target) {
    List<String> mandatoryProperties = new ArrayList<>();
    for (Mapping m : target.getMappings()) {

      if (m.getFragmentType() == entityType) {
        if (m.isMandatory()) {
          mandatoryProperties.add(m.getName());
        }
      }
    }
    return mandatoryProperties;
  }

  public static List<String> getNodeKeyProperties(FragmentType entityType, Target target) {
    List<String> nodeKeyProperties = new ArrayList<>();
    for (Mapping m : target.getMappings()) {
      if (m.getFragmentType() == entityType) {
        if (m.getRole() == RoleType.key) {
          nodeKeyProperties.add(m.getName());
        }
      }
    }
    return nodeKeyProperties;
  }
}
