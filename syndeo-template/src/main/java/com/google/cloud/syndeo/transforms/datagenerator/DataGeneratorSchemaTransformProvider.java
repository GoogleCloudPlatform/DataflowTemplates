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
package com.google.cloud.syndeo.transforms.datagenerator;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.cloud.syndeo.transforms.TypedSchemaTransformProvider;
import com.google.cloud.syndeo.transforms.datagenerator.DataGeneratorSchemaTransformProvider.DataGeneratorSchemaTransformConfiguration;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@AutoService(SchemaTransformProvider.class)
public class DataGeneratorSchemaTransformProvider
    extends TypedSchemaTransformProvider<DataGeneratorSchemaTransformConfiguration> {

  @Override
  public SchemaTransform from(DataGeneratorSchemaTransformConfiguration configuration) {
    org.apache.beam.sdk.schemas.Schema beamSchema;
    SerializableFunction<byte[], Row> valueMapper;
    if (StringUtils.isBlank(configuration.getSchema())) {
      throw new IllegalArgumentException("No schema specified.");
    }

    SchemaTransform transform =
        new DataGeneratorSchemaTransform(
            configuration.getRecordsPerSecond(),
            configuration.getSecondsToRun(),
            configuration.getSchema());
    return transform;
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "syndeo:schematransform:com.google.cloud:data_generator:v1";
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
    return List.of("output");
  }

  @Override
  public Class<DataGeneratorSchemaTransformConfiguration> configurationClass() {
    return DataGeneratorSchemaTransformConfiguration.class;
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class DataGeneratorSchemaTransformConfiguration {

    @SchemaFieldDescription("The number of records generated per second")
    public abstract @Nullable Long getRecordsPerSecond();

    @SchemaFieldDescription("The number of seconds to run the generator.")
    public abstract @Nullable Long getSecondsToRun();

    @SchemaFieldDescription(
        "The AVRO schema string in which the data generated is encoded. "
            + "This is a schema defined with AVRO schema syntax "
            + "(https://avro.apache.org/docs/1.10.2/spec.html#schemas).")
    public abstract String getSchema();

    public static DataGeneratorSchemaTransformProvider.DataGeneratorSchemaTransformConfiguration
            .Builder
        builder() {
      return new AutoValue_DataGeneratorSchemaTransformProvider_DataGeneratorSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract DataGeneratorSchemaTransformProvider.DataGeneratorSchemaTransformConfiguration
              .Builder
          setRecordsPerSecond(Long recordsPerSecond);

      public abstract DataGeneratorSchemaTransformProvider.DataGeneratorSchemaTransformConfiguration
              .Builder
          setSecondsToRun(Long secondsToRun);

      public abstract DataGeneratorSchemaTransformProvider.DataGeneratorSchemaTransformConfiguration
              .Builder
          setSchema(String schema);

      public abstract DataGeneratorSchemaTransformProvider.DataGeneratorSchemaTransformConfiguration
          build();
    }
  }
}
