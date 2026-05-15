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
import org.neo4j.importer.v1.pipeline.CustomQueryTargetStep;
import org.neo4j.importer.v1.pipeline.EntityTargetStep;
import org.neo4j.importer.v1.pipeline.NodeTargetStep;
import org.neo4j.importer.v1.pipeline.RelationshipTargetStep;
import org.neo4j.importer.v1.pipeline.TargetStep;
import org.neo4j.importer.v1.targets.EntityTarget;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.RelationshipTarget;
import org.neo4j.importer.v1.targets.TargetType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility functions for Beam rows and schema. */
public class ModelUtils {
  private static final Pattern variablePattern = Pattern.compile("(\\$([a-zA-Z0-9_]+))");
  private static final Logger LOG = LoggerFactory.getLogger(ModelUtils.class);

  public static boolean targetHasTransforms(TargetStep step) {
    if (step instanceof CustomQueryTargetStep) {
      return false;
    }
    Optional<SourceTransformations> sourceTransformations =
        ((EntityTargetStep) step).extension(SourceTransformations.class);
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

  // note: use scarcely - instanceof is usually better suited
  public static TargetType targetType(TargetStep step) {
    if (step instanceof NodeTargetStep) {
      return TargetType.NODE;
    }
    if (step instanceof RelationshipTargetStep) {
      return TargetType.RELATIONSHIP;
    }
    if (step instanceof CustomQueryTargetStep) {
      return TargetType.QUERY;
    }
    throw new IllegalArgumentException(
        "Could not infer target type from: %s".formatted(step.getClass().getName()));
  }

  public static String getTargetSql(
      TargetStep target, Set<String> fieldNameMap, boolean generateSqlSort) {
    return getTargetSql(target, fieldNameMap, generateSqlSort, null);
  }

  public static String getTargetSql(
      TargetStep target, Set<String> fieldNameMap, boolean generateSqlSort, String baseSql) {

    if (!(target instanceof EntityTargetStep)) {
      throw new IllegalArgumentException(
          String.format(
              "Expected node or relationship target, got %s", target.getClass().getName()));
    }

    Optional<SourceTransformations> transformations =
        ((EntityTargetStep) target).extension(SourceTransformations.class);
    try {
      var statement = new PlainSelect();
      statement.withFromItem(new Table("PCOLLECTION"));
      if (generateSqlSort) {
        List<OrderByElement> sqlOrderBy = new ArrayList<>();
        if (target instanceof RelationshipTargetStep relationshipStep) {
          for (var keyMapping : relationshipStep.endNode().keyProperties()) {
            var escapedSourceField = "`%s`".formatted(keyMapping.getSourceField());
            sqlOrderBy.add(
                new OrderByElement()
                    .withExpression(CCJSqlParserUtil.parseExpression(escapedSourceField)));
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
          var allProperties = getAllPropertyMappings(target);
          Column[] groupByFields =
              allProperties.stream()
                  .map(PropertyMapping::getSourceField)
                  .filter(fieldNameMap::contains)
                  .map(field -> new Column("`%s`".formatted(field)))
                  .toArray(Column[]::new);
          if (groupByFields.length == 0) {
            throw new RuntimeException(
                String.format(
                    "Could not find mapped fields for target: %s. Please verify that target fields exist in source query.",
                    target.name()));
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

  public static List<PropertyMapping> getAllPropertyMappings(TargetStep step) {
    if (!(step instanceof RelationshipTargetStep relationshipTargetStep)) {
      return getPropertyMappings(step);
    }

    Set<PropertyMapping> result = new LinkedHashSet<>(relationshipTargetStep.keyProperties());
    result.addAll(relationshipTargetStep.nonKeyProperties());
    result.addAll(relationshipTargetStep.startNode().keyProperties());
    result.addAll(relationshipTargetStep.endNode().keyProperties());
    return new ArrayList<>(result);
  }

  public static List<PropertyMapping> getPropertyMappings(TargetStep step) {
    if (!(step instanceof EntityTargetStep entityTargetStep)) {
      return List.of();
    }

    Set<PropertyMapping> result = new LinkedHashSet<>(entityTargetStep.keyProperties());
    result.addAll(entityTargetStep.nonKeyProperties());
    return new ArrayList<>(result);
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
    var keys = keyFields.collect(Collectors.toCollection(LinkedHashSet::new));
    if (!keys.isEmpty()) {
      return keys;
    }
    return uniqueFields.collect(Collectors.toCollection(LinkedHashSet::new));
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
