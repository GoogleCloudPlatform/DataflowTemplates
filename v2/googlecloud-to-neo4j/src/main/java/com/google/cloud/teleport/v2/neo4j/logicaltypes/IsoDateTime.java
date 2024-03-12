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
package com.google.cloud.teleport.v2.neo4j.logicaltypes;

import com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import org.apache.beam.sdk.schemas.Schema;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class IsoDateTime implements Schema.LogicalType<TemporalAccessor, String> {
  public static final String IDENTIFIER = "Neo4jIsoDateTime";

  @Override
  public @UnknownKeyFor @NonNull @Initialized String getIdentifier() {
    return IDENTIFIER;
  }

  // unused
  @Override
  public Schema.FieldType getArgumentType() {
    return Schema.FieldType.STRING;
  }

  // unused
  @SuppressWarnings("unchecked")
  @Override
  public @Nullable String getArgument() {
    return "";
  }

  //
  @Override
  public Schema.FieldType getBaseType() {
    return Schema.FieldType.STRING;
  }

  @Override
  public @NonNull String toBaseType(@NonNull TemporalAccessor input) {
    return DateTimeFormatter.ISO_DATE_TIME.format(input);
  }

  @Override
  public @NonNull TemporalAccessor toInputType(@NonNull String base) {
    return DataCastingUtils.asDateTime(base, ZonedDateTime::from, OffsetDateTime::from);
  }
}
