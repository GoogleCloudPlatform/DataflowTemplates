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
  public static final String SPANNER_FOREIGN_KEY = "spannerForeignKey_";
  public static final String SPANNER_INDEX = "spannerIndex_";
  public static final String SPANNER_ON_DELETE_ACTION = "spannerOnDeleteAction";
  public static final String SPANNER_OPTION = "spannerOption_";
  public static final String SPANNER_PARENT = "spannerParent";
  public static final String SPANNER_PRIMARY_KEY = "spannerPrimaryKey";
  public static final String SPANNER_REMOTE = "spannerRemote";
  public static final String SPANNER_SEQUENCE_OPTION = "sequenceOption_";
  public static final String SPANNER_VIEW_QUERY = "spannerViewQuery";
  public static final String SPANNER_VIEW_SECURITY = "spannerViewSecurity";
  public static final String STORED = "stored";

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
}
