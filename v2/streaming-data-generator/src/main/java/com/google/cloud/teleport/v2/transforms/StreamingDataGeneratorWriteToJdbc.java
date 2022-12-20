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
package com.google.cloud.teleport.v2.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator;
import com.google.cloud.teleport.v2.utils.JsonStringToQueryMapper;
import com.google.common.base.Splitter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;

/** A {@link PTransform} converts generatedMessages to write to Jdbc. Table. */
@AutoValue
public abstract class StreamingDataGeneratorWriteToJdbc
    extends PTransform<PCollection<byte[]>, PDone> {

  abstract StreamingDataGenerator.StreamingDataGeneratorOptions getPipelineOptions();

  public static Builder builder(StreamingDataGenerator.StreamingDataGeneratorOptions options) {
    return new AutoValue_StreamingDataGeneratorWriteToJdbc.Builder().setPipelineOptions(options);
  }

  /** Builder for {@link StreamingDataGeneratorWriteToJdbc}. */
  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setPipelineOptions(StreamingDataGenerator.StreamingDataGeneratorOptions value);

    public abstract StreamingDataGeneratorWriteToJdbc build();
  }

  @Override
  public PDone expand(PCollection<byte[]> generatedMessages) {
    StreamingDataGenerator.StreamingDataGeneratorOptions options = getPipelineOptions();
    JdbcIO.DataSourceConfiguration dataSourceConfiguration =
        JdbcIO.DataSourceConfiguration.create(
            options.getDriverClassName(), options.getConnectionUrl());
    if (options.getUsername() != null) {
      dataSourceConfiguration = dataSourceConfiguration.withUsername(options.getUsername());
    }
    if (options.getPassword() != null) {
      dataSourceConfiguration = dataSourceConfiguration.withPassword(options.getPassword());
    }
    if (options.getConnectionProperties() != null) {
      dataSourceConfiguration =
          dataSourceConfiguration.withConnectionProperties(options.getConnectionProperties());
    }

    return generatedMessages
        .apply(
            "Convert to String",
            MapElements.into(TypeDescriptors.strings())
                .via((byte[] element) -> new String(element, StandardCharsets.UTF_8)))
        .apply(
            "Write To Jdbc",
            JdbcIO.<String>write()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withStatement(options.getStatement())
                .withPreparedStatementSetter(
                    new JsonStringToQueryMapper(getKeyOrder(options.getStatement()))));
  }

  private static List<String> getKeyOrder(String statement) {
    int startIndex = statement.indexOf("(");
    int endIndex = statement.indexOf(")");
    String data = statement.substring(startIndex + 1, endIndex);
    return Splitter.on(',').splitToList(data);
  }
}
