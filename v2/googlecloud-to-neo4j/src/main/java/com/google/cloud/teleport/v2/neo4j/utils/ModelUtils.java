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

import com.google.cloud.teleport.v2.neo4j.database.CypherPatterns;
import com.google.cloud.teleport.v2.neo4j.transforms.Aggregation;
import com.google.cloud.teleport.v2.neo4j.transforms.Order;
import com.google.cloud.teleport.v2.neo4j.transforms.OrderBy;
import com.google.cloud.teleport.v2.neo4j.transforms.SourceTransformations;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.importer.v1.targets.EntityTarget;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.RelationshipTarget;
import org.neo4j.importer.v1.targets.Target;
import org.neo4j.importer.v1.targets.TargetType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility functions for Beam rows and schema. */
public class ModelUtils {
  private static final Pattern variablePattern = Pattern.compile("(\\$([a-zA-Z0-9_]+))");
  private static final Logger LOG = LoggerFactory.getLogger(ModelUtils.class);

  public static boolean targetHasTransforms(Target target) {
    if (target.getTargetType() == TargetType.QUERY) {
      return false;
    }
    Optional<SourceTransformations> sourceTransformations =
        ((EntityTarget) target).getExtension(SourceTransformations.class);
    if (sourceTransformations.isEmpty()) {
      return false;
    }
    var transformations = sourceTransformations.get();
    return transformations.enableGrouping()
        || !transformations.aggregations().isEmpty()
        || !transformations.orderByClauses().isEmpty()
        || !transformations.whereClause().isBlank();
  }

  public static Set<String> getBeamFieldSet(Schema schema) {
    return new HashSet<>(schema.getFieldNames());
  }

  public static String getTargetSql(
      Target target,
      NodeTarget startNodeTarget,
      NodeTarget endNodeTarget,
      Set<String> fieldNameMap,
      boolean generateSqlSort) {
    return getTargetSql(
        target, startNodeTarget, endNodeTarget, fieldNameMap, generateSqlSort, null);
  }

  public static String getTargetSql(
      Target target,
      NodeTarget startNodeTarget,
      NodeTarget endNodeTarget,
      Set<String> fieldNameMap,
      boolean generateSqlSort,
      String baseSql) {

    TargetType targetType = target.getTargetType();
    if (targetType != TargetType.NODE && targetType != TargetType.RELATIONSHIP) {
      throw new IllegalArgumentException(
          String.format("Expected node or relationship target, got %s", targetType));
    }

    var transformations = ((EntityTarget) target).getExtension(SourceTransformations.class);
    try {
      var statement = new PlainSelect();
      statement.withFromItem(new Table("PCOLLECTION"));
      if (generateSqlSort) {
        List<OrderByElement> sqlOrderBy = new ArrayList<>();
        if (targetType == TargetType.RELATIONSHIP) {
          var reversedMappings =
              endNodeTarget.getProperties().stream()
                  .collect(
                      Collectors.toMap(
                          PropertyMapping::getTargetProperty, PropertyMapping::getSourceField));
          for (String key : getKeyProperties(endNodeTarget)) {
            String keyField = reversedMappings.get(key);
            String field = CypherPatterns.sanitize(keyField);
            sqlOrderBy.add(
                new OrderByElement().withExpression(CCJSqlParserUtil.parseExpression(field)));
          }
        }
        if (sqlOrderBy.isEmpty() && transformations.isPresent()) {
          var orderBys = transformations.get().orderByClauses();
          if (orderBys != null) {
            for (OrderBy orderByClause : orderBys) {
              sqlOrderBy.add(convertToJsqlElement(orderByClause));
            }
          }
        }

        if (!sqlOrderBy.isEmpty()) {
          statement.withOrderByElements(sqlOrderBy);
        }
      }

      if (transformations.isPresent()) {
        /////////////////////////////////
        // Grouping transform
        var transforms = transformations.get();
        List<Aggregation> aggregations = transforms.aggregations();
        if (transforms.enableGrouping() || aggregations != null && !aggregations.isEmpty()) {
          Set<PropertyMapping> allProperties =
              getAllPropertyMappings((EntityTarget) target, startNodeTarget, endNodeTarget);
          Column[] groupByFields =
              allProperties.stream()
                  .map(PropertyMapping::getSourceField)
                  .filter(fieldNameMap::contains)
                  .map(field -> new Column(CypherPatterns.sanitize(field)))
                  .toArray(Column[]::new);
          if (groupByFields.length == 0) {
            throw new RuntimeException(
                String.format(
                    "Could not find mapped fields for target: %s. Please verify that target fields exist in source query.",
                    target.getName()));
          }
          statement.addSelectItems(groupByFields);
          if (aggregations != null) {
            for (Aggregation aggregation : aggregations) {
              String keyField = aggregation.fieldName();
              statement.addSelectItem(
                  CCJSqlParserUtil.parseExpression(aggregation.expression()),
                  new Alias(CypherPatterns.sanitize(keyField)));
            }
          }

          String whereClause = transforms.whereClause();
          if (StringUtils.isNotBlank(whereClause)) {
            statement.withWhere(CCJSqlParserUtil.parseExpression(whereClause));
          }
          for (Column groupByField : groupByFields) {
            statement.addGroupByColumnReference(groupByField);
          }
          var limit = transforms.limit();
          if (limit >= 0) {
            statement.setLimit(new Limit().withRowCount(new LongValue(limit)));
          }
        }
      }

      if (statement.getSelectItems() == null || statement.getSelectItems().isEmpty()) {
        statement.addSelectItems(new AllColumns());
      }

      String statementText = statement.toString();
      if (StringUtils.isNotBlank(baseSql)) {
        statementText = statementText.replace("PCOLLECTION", String.format("(%s)", baseSql));
      }
      return statementText;
    } catch (JSQLParserException e) {
      throw new RuntimeException(e);
    }
  }

  public static Set<PropertyMapping> getAllPropertyMappings(EntityTarget entityTarget) {
    return getAllPropertyMappings(entityTarget, null, null);
  }

  public static Set<PropertyMapping> getAllPropertyMappings(
      EntityTarget entityTarget, NodeTarget startNodeTarget, NodeTarget endNodeTarget) {
    Set<PropertyMapping> result = new LinkedHashSet<>(entityTarget.getProperties());
    if (startNodeTarget != null && endNodeTarget != null) {
      result.addAll(startNodeTarget.getProperties());
      result.addAll(endNodeTarget.getProperties());
    }
    return result;
  }

  public static String replaceVariableTokens(String text, Map<String, String> replacements) {
    Matcher matcher = variablePattern.matcher(text);
    // populate the replacements map ...
    StringBuilder builder = new StringBuilder();
    int i = 0;
    while (matcher.find()) {
      LOG.debug("Matcher group: " + matcher.group(1));
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
    LOG.debug("Before: " + text + ", after: " + replacedText);
    return replacedText;
  }

  public static Set<String> getKeyProperties(EntityTarget entity) {
    Stream<String> keyFields;
    Stream<String> uniqueFields;
    if (entity instanceof NodeTarget nodeTarget) {
      keyFields =
          nodeTarget.getSchema().getKeyConstraints().stream()
              .flatMap(constraint -> constraint.getProperties().stream());
      uniqueFields =
          nodeTarget.getSchema().getUniqueConstraints().stream()
              .flatMap(constraint -> constraint.getProperties().stream());
    } else if (entity instanceof RelationshipTarget relationshipTarget) {
      keyFields =
          relationshipTarget.getSchema().getKeyConstraints().stream()
              .flatMap(constraint -> constraint.getProperties().stream());
      uniqueFields =
          relationshipTarget.getSchema().getUniqueConstraints().stream()
              .flatMap(constraint -> constraint.getProperties().stream());
    } else {
      throw new IllegalArgumentException(
          "Expected node or relationship target when gathering key properties, found: %s"
              .formatted(entity.getClass()));
    }
    return Stream.concat(keyFields, uniqueFields)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private static OrderByElement convertToJsqlElement(OrderBy orderByClause)
      throws JSQLParserException {
    var element =
        new OrderByElement()
            .withExpression(CCJSqlParserUtil.parseExpression(orderByClause.expression()));
    var order = orderByClause.order();
    if (order == null) {
      return element;
    }
    return element.withAscDescPresent(true).withAsc(order == Order.ASC);
  }
}
