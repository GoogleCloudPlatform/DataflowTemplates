/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.spanner;

import java.util.List;
import org.apache.avro.Schema;

/** Avro utilities. */
public class AvroUtil {
  private AvroUtil() {}

  // The property names in Avro schema.
  public static final String DEFAULT_EXPRESSION = "defaultExpression";
  public static final String GENERATION_EXPRESSION = "generationExpression";
  public static final String GOOGLE_FORMAT_VERSION = "googleFormatVersion";
  public static final String GOOGLE_STORAGE = "googleStorage";
  public static final String INPUT = "Input";
  public static final String NOT_NULL = "notNull";
  public static final String OUTPUT = "Output";
  public static final String SQL_TYPE = "sqlType";
  public static final String SPANNER_CHECK_CONSTRAINT = "spannerCheckConstraint_";
  public static final String SPANNER_CHANGE_STREAM_FOR_CLAUSE = "spannerChangeStreamForClause";
  public static final String SPANNER_ENTITY = "spannerEntity";
  public static final String SPANNER_ENTITY_MODEL = "Model";
  public static final String SPANNER_ENTITY_PROPERTY_GRAPH = "PropertyGraph";
  public static final String SPANNER_ENTITY_PLACEMENT = "Placement";
  public static final String SPANNER_FOREIGN_KEY = "spannerForeignKey_";
  public static final String SPANNER_INDEX = "spannerIndex_";
  public static final String SPANNER_ON_DELETE_ACTION = "spannerOnDeleteAction";
  public static final String SPANNER_OPTION = "spannerOption_";
  public static final String SPANNER_PARENT = "spannerParent";
  public static final String SPANNER_PRIMARY_KEY = "spannerPrimaryKey";
  public static final String SPANNER_REMOTE = "spannerRemote";
  public static final String SPANNER_SEQUENCE_OPTION = "sequenceOption_";
  public static final String SPANNER_SEQUENCE_KIND = "sequenceKind";
  public static final String SPANNER_SEQUENCE_SKIP_RANGE_MIN = "skipRangeMin";
  public static final String SPANNER_SEQUENCE_SKIP_RANGE_MAX = "skipRangeMax";
  public static final String SPANNER_SEQUENCE_COUNTER_START = "counterStartValue";
  public static final String SPANNER_VIEW_QUERY = "spannerViewQuery";
  public static final String SPANNER_VIEW_SECURITY = "spannerViewSecurity";
  public static final String SPANNER_NAMED_SCHEMA = "spannerNamedSchema";

  public static final String SPANNER_NODE_TABLE = "spannerGraphNodeTable";
  public static final String SPANNER_EDGE_TABLE = "spannerGraphEdgeTable";
  public static final String SPANNER_PROPERTY_DECLARATION = "spannerGraphPropertyDeclaration";
  public static final String SPANNER_LABEL = "spannerGraphLabel";

  public static final String SPANNER_NAME = "spannerName";
  public static final String STORED = "stored";
  public static final String SPANNER_PLACEMENT_KEY = "spannerPlacementKey";
  public static final String HIDDEN = "hidden";

  public static Schema unpackNullable(Schema schema) {
    if (schema.getType() != Schema.Type.UNION) {
      return null;
    }
    List<Schema> unionTypes = schema.getTypes();
    if (unionTypes.size() != 2) {
      return null;
    }

    if (unionTypes.get(0).getType() == Schema.Type.NULL) {
      return unionTypes.get(1);
    }
    if (unionTypes.get(1).getType() == Schema.Type.NULL) {
      return unionTypes.get(0);
    }
    return null;
  }

  /**
   * For named schema, the database object name could have dot(.). When building a avro schema
   * object, a name with dot(.) is special. See org.apache.avro.Schema.Name#name for more detail. So
   * we use avro property to store the database object name in newer version, this function thus
   * provides a way to get name in a compatible way.
   *
   * <p>For example, if a table's name is `foo` and not in named schema. Both
   * `schema.getProp(SPANNER_NAME)` and `schema.getName()` will be `foo`. getSpannerObjectName()
   * will return `foo`
   *
   * <p>If a table's name is `foo` and in named schema `sch1`. `schema.getProp(SPANNER_NAME)` is
   * `sch1.foo`. `schema.getName()` is `sch1_foo`. getSpannerObjectName() will return `sch1.foo`
   *
   * @return spanner's database object name.
   */
  public static String getSpannerObjectName(Schema schema) {
    if (schema.getProp(SPANNER_NAME) != null && !schema.getProp(SPANNER_NAME).isEmpty()) {
      return schema.getProp(SPANNER_NAME);
    }
    return schema.getName();
  }

  /**
   * For a fully qualified database object name, we can not use it directly as avro schema name.
   * Transform the name by replacing the dot(.) using underscore(_) for two purpose 1. avoid
   * conflicts with avro and 2. make sure it is unique in avro.
   *
   * <p>If a table's fully qualified name is sch1.foo, then the Avro schema name will be `sch1_foo`
   *
   * @param spannerName database object name
   * @return avro schema name.
   */
  public static String generateAvroSchemaName(String spannerName) {
    if (spannerName.contains(".")) {
      return spannerName.replaceAll("\\.", "_");
    }
    return spannerName;
  }
}
