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
package com.google.cloud.teleport.v2.reader.io.schema.typemapping;

import com.google.cloud.teleport.v2.reader.io.schema.typemapping.provider.unified.Unsupported;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/**
 * Maps the source schema to unified avro type.
 *
 * <p>//TODO- check if this can be rolled into the connector.
 *
 * @see <a href = https://cloud.google.com/datastream/docs/unified-types> Mappings of unified types
 *     to source and destination data types</a>
 */
public final class UnifiedTypeMapper implements Serializable {

  private final ImmutableMap<String, UnifiedTypeMapping> mapping;

  /**
   * Constructs the {@link UnifiedTypeMapper}.
   *
   * @param mapping type mapping for source database types.
   */
  public UnifiedTypeMapper(ImmutableMap<String, UnifiedTypeMapping> mapping) {
    Preconditions.checkNotNull(mapping, "Mapping cannot be null");
    this.mapping = mapping;
  }

  /**
   * Returns the {@link Schema Avro Schema} for the given sourceColumType.
   *
   * @param columnType the details of the source column schema as read from information schema.
   * @return {@link Schema Avro Schema}
   *     <p><b>Note:</b>
   *     <p>The Schema returned is always unioned with nullable type irrespective of whether the
   *     source column has a <code>not null</code> constraint.
   *     <ol>
   *       <li>This accommodates cases where a not-null constraint was added by altering a column
   *           without validation. Refer to <a
   *           href=https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-DESC-ADD-TABLE-CONSTRAINT>PG
   *           Alter table documentation</a> for an example.
   *       <li>Other produces that follow the Unified Mapping, also use nullable union with the
   *           documented schema
   *       <li>The <code>reader</code> will not drop null (or any other) values returned by the
   *           source db that violate the source db's schema constraint. The down stream pipeline
   *           may choose to transform them, mark them as severe errors, or even write them to
   *           spanner if spanner schema does not have the same constraint.
   *     </ol>
   */
  public Schema getSchema(SourceColumnType columnType) {
    Schema schema = getBasicSchema(columnType);
    return (schema.getType().equals(Schema.Type.NULL))
        ? schema
        : SchemaBuilder.builder().unionOf().nullType().and().type(schema).endUnion();
  }

  /*
   * TODO(vardhanvthigle): Handle Nested collections.
   */
  private Schema getBasicSchema(SourceColumnType columnType) {
    return mapping
        .getOrDefault(columnType.getName().toUpperCase(), new Unsupported())
        .getSchema(columnType.getMods(), columnType.getArrayBounds());
  }
}
