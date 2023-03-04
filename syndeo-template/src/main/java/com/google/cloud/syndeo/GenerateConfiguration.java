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
package com.google.cloud.syndeo;

import com.google.cloud.datapipelines.v1.syndeo.BatchGetTransformDescriptionsResponse;
import com.google.cloud.datapipelines.v1.syndeo.MapType;
import com.google.cloud.datapipelines.v1.syndeo.TransformDescription;
import com.google.cloud.datapipelines.v1.syndeo.TypeName;
import com.google.cloud.syndeo.common.ProviderUtil;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility that generates Proto Text configuration parameters for supported {@code
 * SchemaTransform} implementations in the classpath.
 *
 * <p>To run:
 *
 * <p>{@code mvn compile exec:java -pl syndeo-template/pom.xml \
 * -Dexec.mainClass="com.google.cloud.syndeo.GenerateConfiguration" \ -Dexec.args="samplyfile" }
 */
public class GenerateConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(GenerateConfiguration.class);

  public static void main(String[] args) throws IOException {
    Collection<SchemaTransformProvider> providers = ProviderUtil.getProviders();
    List<TransformDescription> descriptions =
        providers.stream()
            .filter(provider -> SyndeoTemplate.SUPPORTED_URNS.containsKey(provider.identifier()))
            .map(GenerateConfiguration::providerToConfiguration)
            .collect(Collectors.toList());

    String fileName = args.length > 0 ? args[0] : "transform_configs.prototext";
    Files.write(
        Paths.get(fileName),
        BatchGetTransformDescriptionsResponse.newBuilder()
            .addAllTransformDescriptions(descriptions)
            .build()
            .toString()
            .getBytes(StandardCharsets.UTF_8));
  }

  static com.google.cloud.datapipelines.v1.syndeo.TransformDescription providerToConfiguration(
      SchemaTransformProvider provider) {
    Set<String> supportedFields = SyndeoTemplate.SUPPORTED_URNS.get(provider.identifier());
    LOG.info("Generating configuration for {}", provider.identifier());
    try {
      TransformDescription.Builder builder =
          TransformDescription.newBuilder()
              .setName(provider.identifier())
              .setUniformResourceName(provider.identifier())
              .setOptions(
                  datapipelinesFieldTypeFromBeamSchemaFieldType(
                      provider.configurationSchema(), supportedFields));
      return builder.build();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to generate configuration for transform provider %s", provider.identifier()),
          e);
    }
  }

  static com.google.cloud.datapipelines.v1.syndeo.Schema
      datapipelinesFieldTypeFromBeamSchemaFieldType(
          Schema beamSchema, Set<String> supportedFields) {
    com.google.cloud.datapipelines.v1.syndeo.Schema.Builder schemaBuilder =
        com.google.cloud.datapipelines.v1.syndeo.Schema.newBuilder();
    for (Schema.Field f : beamSchema.getFields()) {
      if (supportedFields.size() > 0 && !supportedFields.contains(f.getName())) {
        // This field is not supported. We will skip it.
        continue;
      }
      try {
        schemaBuilder.addFields(
            com.google.cloud.datapipelines.v1.syndeo.Field.newBuilder()
                .setName(f.getName())
                .setType(datapipelinesFieldTypeFromBeamSchemaFieldType(f.getType()))
                .build());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format("Unable to convert schema field %s for schema %s", f, beamSchema), e);
      }
    }
    return schemaBuilder.build();
  }

  static com.google.cloud.datapipelines.v1.syndeo.FieldType
      datapipelinesFieldTypeFromBeamSchemaFieldType(Schema.FieldType beamFieldType) {
    com.google.cloud.datapipelines.v1.syndeo.FieldType.Builder typeBuilder =
        com.google.cloud.datapipelines.v1.syndeo.FieldType.newBuilder();
    typeBuilder = typeBuilder.setNullable(beamFieldType.getNullable());
    switch (beamFieldType.getTypeName()) {
      case STRING:
        return typeBuilder.setType(TypeName.TYPE_NAME_STRING).build();
      case BYTE:
        return typeBuilder.setType(TypeName.TYPE_NAME_BYTE).build();
      case BYTES:
        return typeBuilder.setType(TypeName.TYPE_NAME_BYTES).build();
      case INT16:
        return typeBuilder.setType(TypeName.TYPE_NAME_INT16).build();
      case INT32:
        return typeBuilder.setType(TypeName.TYPE_NAME_INT32).build();
      case INT64:
        return typeBuilder.setType(TypeName.TYPE_NAME_INT64).build();
      case FLOAT:
        return typeBuilder.setType(TypeName.TYPE_NAME_FLOAT).build();
      case DOUBLE:
        return typeBuilder.setType(TypeName.TYPE_NAME_DOUBLE).build();
      case BOOLEAN:
        return typeBuilder.setType(TypeName.TYPE_NAME_BOOLEAN).build();
      case DATETIME:
        return typeBuilder.setType(TypeName.TYPE_NAME_DATETIME).build();
      case DECIMAL:
        return typeBuilder.setType(TypeName.TYPE_NAME_DECIMAL).build();
      case ARRAY:
        return typeBuilder
            .setType(TypeName.TYPE_NAME_ARRAY)
            .setCollectionElementType(
                datapipelinesFieldTypeFromBeamSchemaFieldType(
                    Objects.requireNonNull(beamFieldType.getCollectionElementType())))
            .build();
      case ITERABLE:
        return typeBuilder
            .setType(TypeName.TYPE_NAME_ITERABLE)
            .setCollectionElementType(
                datapipelinesFieldTypeFromBeamSchemaFieldType(
                    Objects.requireNonNull(beamFieldType.getCollectionElementType())))
            .build();
      case ROW:
        return typeBuilder
            .setType(TypeName.TYPE_NAME_ROW)
            .setRowSchema(
                datapipelinesFieldTypeFromBeamSchemaFieldType(
                    Objects.requireNonNull(beamFieldType.getRowSchema()), new HashSet<>()))
            .build();
      case MAP:
        return typeBuilder
            .setType(TypeName.TYPE_NAME_MAP)
            .setMapType(
                MapType.newBuilder()
                    .setMapKeyType(
                        datapipelinesFieldTypeFromBeamSchemaFieldType(
                            Objects.requireNonNull(beamFieldType.getMapKeyType())))
                    .setMapValueType(
                        datapipelinesFieldTypeFromBeamSchemaFieldType(
                            Objects.requireNonNull(beamFieldType.getMapValueType())))
                    .build())
            .build();
      default:
        throw new IllegalArgumentException(
            String.format("Unable to convert Beam type %s to Datapipelines type.", beamFieldType));
    }
  }
}
