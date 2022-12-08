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
package com.google.cloud.syndeo.common;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.schemas.logicaltypes.SchemaLogicalType;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/**
 * A class that can wrap a {@link SchemaIO} and {@link SchemaIOProvider} as a read or write {@link
 * SchemaTransformProvider}.
 *
 * <p>TODO(BEAM-14021): This class can be removed when all SchemaIOs are converted to
 * SchemaTransform.
 */
public class SchemaIOTransformProviderWrapper implements SchemaTransformProvider {

  /** The underlying provider. */
  SchemaIOProvider provider;

  /** True if this transform provider is for a read, false for write. */
  boolean isRead;

  public SchemaIOTransformProviderWrapper(SchemaIOProvider provider, boolean isRead) {
    this.provider = provider;
    this.isRead = isRead;
  }

  /** Uses AutoService to load all SchemaIOs and return them as SchemaTransformProviders. */
  public static List<SchemaTransformProvider> getAll() {
    ServiceLoader<SchemaIOProvider> providers = ServiceLoader.load(SchemaIOProvider.class);
    List<SchemaTransformProvider> list =
        StreamSupport.stream(providers.spliterator(), false)
            .map((SchemaIOProvider p) -> new SchemaIOTransformProviderWrapper(p, true))
            .collect(Collectors.toList());

    list.addAll(
        StreamSupport.stream(providers.spliterator(), false)
            .map((SchemaIOProvider p) -> new SchemaIOTransformProviderWrapper(p, false))
            .collect(Collectors.toList()));

    return list;
  }

  @Override
  public String identifier() {
    return "schemaIO:" + provider.identifier() + (isRead ? ":read" : ":write");
  }

  @Override
  public Schema configurationSchema() {
    // Turn the schema and location into actual configuration fields.
    if (provider.requiresDataSchema()) {
      Schema.Builder builder = Schema.builder();
      for (Schema.Field field : provider.configurationSchema().getFields()) {
        builder.addField(field);
      }
      builder.addNullableField("schema", Schema.FieldType.logicalType(new SchemaLogicalType()));
      return builder.build();
    } else {
      return provider.configurationSchema();
    }
  }

  @Override
  public SchemaTransform from(Row configuration) {
    Row.Builder rowBuilder = Row.withSchema(configurationSchema());
    for (int i = 0; i < configurationSchema().getFieldCount(); i++) {
      rowBuilder.addValue(configuration.getValue(i));
    }

    String location =
        configuration.getSchema().hasField("location") ? configuration.getString("location") : null;
    Schema schema =
        configuration.getSchema().hasField("schema")
            ? configuration.getLogicalTypeValue("schema", Schema.class)
            : null;
    if (schema == null && provider.requiresDataSchema()) {
      throw new IllegalArgumentException("No schema provided for SchemaIO that requires schema.");
    }
    SchemaIO schemaIO = provider.from(location, rowBuilder.build(), schema);
    return isRead ? new SchemaIOToReadTransform(schemaIO) : new SchemaIOToWriteTransform(schemaIO);
  }

  @Override
  public List<String> inputCollectionNames() {
    return isRead ? Arrays.asList() : Arrays.asList("input");
  }

  @Override
  public List<String> outputCollectionNames() {
    return isRead ? Arrays.asList("output") : Arrays.asList();
  }

  /** A class that exposes {@link SchemaIO} as a read {@link SchemaTransform}. */
  private static class SchemaIOToReadTransform implements SchemaTransform, Serializable {
    SchemaIO schemaIO;

    private SchemaIOToReadTransform(SchemaIO schemaIO) {
      this.schemaIO = schemaIO;
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
        @Override
        public PCollectionRowTuple expand(PCollectionRowTuple input) {
          if (!input.getAll().isEmpty()) {
            throw new InvalidConfigurationException(
                "Unexpected input transform: SchemaIO read's should not have an input.");
          }
          // If the transform does not have a schema as part of its configuration, then
          // the transform itself will infer the schema and automatically return it.
          if (schemaIO.schema() == null) {
            return PCollectionRowTuple.of(
                "output", input.getPipeline().apply(schemaIO.buildReader()));
          } else {
            return PCollectionRowTuple.of(
                "output",
                input.getPipeline().apply(schemaIO.buildReader()).setRowSchema(schemaIO.schema()));
          }
        }
      };
    }
  }

  /** A class that exposes {@link SchemaIO} as a write {@link SchemaTransform}. */
  private static class SchemaIOToWriteTransform implements SchemaTransform, Serializable {
    SchemaIO schemaIO;

    private SchemaIOToWriteTransform(SchemaIO schemaIO) {
      this.schemaIO = schemaIO;
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
        @Override
        public PCollectionRowTuple expand(PCollectionRowTuple inputs) {
          PCollection<Row> input = inputs.get("input");
          // Verify that the input schema matches what we expect.
          // And error a proper error otherwise.
          Preconditions.checkArgument(
              schemaIO.schema() == null || input.getSchema().equals(schemaIO.schema()),
              "Input collection schema does not match expected schema (input: %s fields, expected: %s fields)",
              input.getSchema().getFields().stream()
                  .map(f -> f.getName())
                  .collect(Collectors.toList()),
              schemaIO.schema() == null
                  ? "null"
                  : schemaIO.schema().getFields().stream()
                      .map(f -> f.getName())
                      .collect(Collectors.toList()));
          input.apply(schemaIO.buildWriter());
          return PCollectionRowTuple.empty(input.getPipeline());
        }
      };
    }
  }
}
