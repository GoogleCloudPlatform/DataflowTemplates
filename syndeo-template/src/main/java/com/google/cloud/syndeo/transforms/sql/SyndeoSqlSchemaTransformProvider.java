/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.syndeo.transforms.sql;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.cloud.syndeo.transforms.TypedSchemaTransformProvider;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.expansion.SqlTransformSchemaTransformProvider;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@AutoService(SchemaTransformProvider.class)
public class SyndeoSqlSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        SyndeoSqlSchemaTransformProvider.SyndeoSqlSchemaTransformConfiguration> {
  @Override
  public Class configurationClass() {
    return SyndeoSqlSchemaTransformConfiguration.class;
  }

  @Override
  public SchemaTransform from(SyndeoSqlSchemaTransformConfiguration configuration) {
    SqlTransformSchemaTransformProvider provider = new SqlTransformSchemaTransformProvider();

    EnumerationType enumtype =
        ((EnumerationType)
            provider.configurationSchema().getField("dialect").getType().getLogicalType());

    return provider.from(
        Row.withSchema(provider.configurationSchema())
            .withFieldValue("query", configuration.getQuery())
            .withFieldValue("dialect", enumtype.valueOf("zetasql"))
            .build());
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "syndeo:schematransform:com.google.cloud:sql_transform:v1";
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return Collections.singletonList("input");
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
    return List.of("output", "errors");
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class SyndeoSqlSchemaTransformConfiguration {
    public abstract String getQuery();
  }
}
