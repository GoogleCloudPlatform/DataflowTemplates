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
package com.google.cloud.teleport.v2.utils;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Schema;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1SchemaPartitionField;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1SchemaSchemaField;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1StorageFormat;
import com.google.cloud.teleport.v2.clients.DataplexClient;
import com.google.cloud.teleport.v2.options.DataplexBigQueryToGcsOptions;
import com.google.cloud.teleport.v2.utils.FileFormat.FileFormatOptions;
import com.google.cloud.teleport.v2.values.BigQueryTable;
import com.google.cloud.teleport.v2.values.DataplexCompression;
import com.google.cloud.teleport.v2.values.DataplexEnums.FieldMode;
import com.google.cloud.teleport.v2.values.DataplexEnums.FieldType;
import com.google.cloud.teleport.v2.values.DataplexEnums.PartitionStyle;
import com.google.cloud.teleport.v2.values.DataplexEnums.StorageFormat;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

/** Provides utility methods for working with Dataplex. */
public class DataplexUtils {

  private static final Pattern ZONE_PATTERN =
      Pattern.compile("projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+");

  /** Gets the zone name from {@code assetName}. */
  public static String getZoneFromAsset(String assetName) {
    Matcher matcher = ZONE_PATTERN.matcher(assetName);
    if (matcher.find()) {
      return matcher.group();
    }
    throw new IllegalArgumentException(
        String.format("Asset '%s' not properly formatted", assetName));
  }

  public static String getShortAssetNameFromAsset(String assetName) {
    return assetName.substring(assetName.lastIndexOf('/') + 1);
  }

  public static GoogleCloudDataplexV1Schema toDataplexSchema(Schema schema, String partitionKey) {
    GoogleCloudDataplexV1Schema result = new GoogleCloudDataplexV1Schema();

    result.setFields(
        schema.getFields().stream()
            .filter(avroField -> !avroField.name().equals(partitionKey))
            .map(DataplexUtils::toDataplexSchemaField)
            .collect(Collectors.toList()));

    if (partitionKey != null && !partitionKey.isEmpty()) {
      Schema.Field avroField = schema.getField(partitionKey);
      if (avroField == null) {
        throw new IllegalArgumentException(
            String.format(
                "Partition key %s not found in the schema definition: %s", partitionKey, schema));
      }

      result.setPartitionFields(Arrays.asList(toDataplexPartitionField(avroField)));
    }

    return result;
  }

  /**
   * Sets additional attributes in the Dataplex schema to make it "hive-compatible".
   *
   * <p>Sets {@code partitionStyle} to "HIVE_COMPATIBLE" and allows overriding the partition field
   * type and name. Foe example, Dataplex supports only STRING partition keys for tables in GCS even
   * if the actual key type is something else (e.g. timestamp). Renaming may be required if the key
   * name in the file path is different from the key name in the file (see {@link
   * DataplexBigQueryToGcsOptions#getEnforceSamePartitionKey()}. The partition field name in the
   * Dataplex schema has to match the field name in the file path, not the field name in the file
   * schema.
   *
   * @param schema schema for the in-place update
   * @param originalPartitionKey the original partition field name to rename, required if {@code
   *     renameToPartitionKey} is set
   * @param renameToPartitionKey the field name to rename {@code originalPartitionKey} to, optional
   * @param overrideFieldType if provided, the type of all partition fields in the schema will be
   *     set to this, optional
   */
  public static void applyHiveStyle(
      GoogleCloudDataplexV1Schema schema,
      @Nullable String originalPartitionKey,
      @Nullable String renameToPartitionKey,
      @Nullable FieldType overrideFieldType) {
    if (renameToPartitionKey != null) {
      checkNotNull(
          originalPartitionKey,
          "originalPartitionKey is required when renameToPartitionKey is set");
    }

    schema.setPartitionStyle(PartitionStyle.HIVE_COMPATIBLE.name());
    if (schema.getPartitionFields() != null
        && (renameToPartitionKey != null || overrideFieldType != null)) {
      for (GoogleCloudDataplexV1SchemaPartitionField f : schema.getPartitionFields()) {
        if (overrideFieldType != null) {
          f.setType(overrideFieldType.name());
        }
        if (renameToPartitionKey != null && originalPartitionKey.equals(f.getName())) {
          f.setName(renameToPartitionKey);
        }
      }
    }
  }

  /**
   * Sets additional attributes in the Dataplex schema to make it "hive-compatible".
   *
   * <p>Same as {@link #applyHiveStyle(GoogleCloudDataplexV1Schema, String, String, FieldType)}, but
   * derives the required parameters from the source BigQuery table and the BigQueryToGcs template
   * naming strategy.
   *
   * <p>Overrides partition field type to STRING as Dataplex supports only that for files on GCS.
   *
   * <p>No schema changes are made if {@code table} is not partitioned.
   *
   * @param schema schema for the in-place update
   * @param table source BigQuery table
   * @param directoryNaming BigQueryToGcs template naming strategy
   */
  public static void applyHiveStyle(
      GoogleCloudDataplexV1Schema schema,
      BigQueryTable table,
      BigQueryToGcsDirectoryNaming directoryNaming) {
    if (table.isPartitioned()) {
      String partitionKey = table.getPartitioningColumn();
      String renameTo = directoryNaming.getPartitionKey(partitionKey);
      DataplexUtils.applyHiveStyle(schema, partitionKey, renameTo, FieldType.STRING);
    }
  }

  private static GoogleCloudDataplexV1SchemaSchemaField toDataplexSchemaField(
      Schema.Field avroField) {
    GoogleCloudDataplexV1SchemaSchemaField f = new GoogleCloudDataplexV1SchemaSchemaField();
    f.setName(avroField.name());
    f.setType(dataplexFieldType(avroField).name());
    f.setMode(dataplexFieldMode(avroField).name());
    if (avroField.schema().getType() == Schema.Type.RECORD) {
      // Handle nested records.
      f.setFields(
          avroField.schema().getFields().stream()
              .map(DataplexUtils::toDataplexSchemaField)
              .collect(Collectors.toList()));
    }
    return f;
  }

  private static GoogleCloudDataplexV1SchemaPartitionField toDataplexPartitionField(
      Schema.Field avroField) {
    GoogleCloudDataplexV1SchemaPartitionField f = new GoogleCloudDataplexV1SchemaPartitionField();
    f.setName(avroField.name());
    f.setType(dataplexFieldType(avroField).name());
    // Dataplex's PartitionField doesn't support nested records, so no special logic here.
    return f;
  }

  private static FieldMode dataplexFieldMode(Schema.Field f) {
    /*
     Field modes supported by Dataplex:

     MODE_UNSPECIFIED	Mode unspecified.
     REQUIRED	        The field has required semantics.
     NULLABLE	        The field has optional semantics, and may be null.
     REPEATED	        The field has repeated (0 or more) semantics, and is a list of values.
    */

    Schema.Type type = f.schema().getType();
    if (type == Schema.Type.ARRAY) {
      return FieldMode.REPEATED;
    } else if (type == Schema.Type.UNION) {
      for (Schema innerSchema : f.schema().getTypes()) {
        if (innerSchema.getType() == Schema.Type.NULL) {
          return FieldMode.NULLABLE;
        }
      }
    }
    return FieldMode.REQUIRED;
  }

  private static FieldType dataplexFieldType(Schema.Field field) {
    /*
    Field types supported by Dataplex:

    TYPE_UNSPECIFIED SchemaType unspecified.
    BOOLEAN	         Boolean field.
    BYTE             Single byte numeric field.
    INT16	           16-bit numeric field.
    INT32	           32-bit numeric field.
    INT64	           64-bit numeric field.
    FLOAT	           Floating point numeric field.
    DOUBLE	         Double precision numeric field.
    DECIMAL	         Real value numeric field.
    STRING	         Sequence of characters field.
    BINARY	         Sequence of bytes field.
    TIMESTAMP	       Date and time field.
    DATE	           Date field.
    TIME	           Time field.
    RECORD	         Structured field. Nested fields that define the structure of the map. If all nested fields are nullable, this field represents a union.
    NULL	           Null field that does not have values.
    */

    Schema schema = field.schema();

    if (schema.getType() == Schema.Type.UNION) {
      // Special case for UNION: a union of ["null", "non-null type"] means this is a
      // nullable field. In Dataplex this will be a field with Mode = NULLABLE and Type = <non-null
      // type>. So here we have to return the type of the other, non-NULL, element.
      // A union of 3+ elements is not supported (can't be represented as a Dataplex type).
      if (schema.getTypes() != null && schema.getTypes().size() == 2) {
        Schema s1 = schema.getTypes().get(0);
        Schema s2 = schema.getTypes().get(1);
        if (s1.getType() == Schema.Type.NULL) {
          return dataplexPrimitiveFieldType(s2);
        } else if (s2.getType() == Schema.Type.NULL) {
          return dataplexPrimitiveFieldType(s1);
        }
      }
      return FieldType.TYPE_UNSPECIFIED;
    }

    if (schema.getType() == Schema.Type.ARRAY) {
      // Special case for ARRAY: check the type of the underlying elements.
      // In Dataplex this will be a field with Mode = REPEATED and Type = <array element type>.
      return dataplexPrimitiveFieldType(schema.getElementType());
    }

    return dataplexPrimitiveFieldType(schema);
  }

  private static FieldType dataplexPrimitiveFieldType(Schema schema) {
    if (schema.getLogicalType() != null) {
      FieldType result = dataplexLogicalFieldType(schema);
      if (result != null) {
        return result;
      }
    }

    Schema.Type avroType = schema.getType();
    switch (avroType) {
      case RECORD:
        return FieldType.RECORD;
      case STRING:
        return FieldType.STRING;
      case FLOAT:
        return FieldType.FLOAT;
      case DOUBLE:
        return FieldType.DOUBLE;
      case BOOLEAN:
        return FieldType.BOOLEAN;
      case NULL:
        return FieldType.NULL;
      case FIXED: // FIXED is binary data with fixed size.
      case BYTES: // BYTES is binary data with variable size.
        return FieldType.BINARY;
      case INT:
        return FieldType.INT32;
      case LONG:
        return FieldType.INT64;
      case UNION: // Shouldn't happen. Unions can not contain other unions as per Avro spec.
      case ARRAY: // Not supported as a primitive type (e.g. if this is an ARRAY of ARRAYs).
      case MAP:
      case ENUM:
      default:
        return FieldType.TYPE_UNSPECIFIED;
    }
  }

  private static FieldType dataplexLogicalFieldType(Schema schema) {
    LogicalType logicalType = schema.getLogicalType();

    if (logicalType instanceof LogicalTypes.Decimal) {
      return FieldType.DECIMAL;
    } else if (logicalType instanceof LogicalTypes.Date) {
      return FieldType.DATE;
    } else if (logicalType instanceof LogicalTypes.TimeMicros
        || logicalType instanceof LogicalTypes.TimeMillis) {
      return FieldType.TIME;
    } else if (logicalType instanceof LogicalTypes.TimestampMicros
        || logicalType instanceof LogicalTypes.TimestampMillis) {
      return FieldType.TIMESTAMP;
    }

    return null;
  }

  public static StorageFormat toDataplexFileFormat(FileFormatOptions o) {
    switch (o) {
      case ORC:
        return StorageFormat.ORC;
      case AVRO:
        return StorageFormat.AVRO;
      case PARQUET:
        return StorageFormat.PARQUET;
      default:
        return StorageFormat.UNKNOWN;
    }
  }

  public static GoogleCloudDataplexV1StorageFormat storageFormat(
      FileFormatOptions fileFormat, DataplexCompression fileCompression) {
    GoogleCloudDataplexV1StorageFormat format = new GoogleCloudDataplexV1StorageFormat();
    format.setMimeType(toDataplexFileFormat(fileFormat).getMimeType());
    format.setCompressionFormat(fileCompression.getDataplexCompressionName());
    return format;
  }

  /**
   * Creates the provided {@code entity}, but if its ID already exists generates a new one.
   *
   * <p>First tries to create an entity with the provided ID (e.g. "foo"). If the API call fails
   * because such ID already exists, tries to create an entity with ID "foo_2", then "foo_3", etc.
   * This mimics the behavior of Dataplex when it auto-discovers entities.
   *
   * @param dataplex the client to use to call Dataplex API
   * @param zoneName example:
   *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
   * @param entity the entity to create, {@link GoogleCloudDataplexV1Entity#getName() name} must be
   *     empty and {@link GoogleCloudDataplexV1Entity#getId() id} must be not empty
   * @param maxAttempts how many attempts to do, e.g. if set to 3 and entity ID is "foo", will fail
   *     after trying to create entities with IDs "foo", "foo_2", and "foo_3"
   * @return the created instance as returned by Dataplex, with the new generated {@link
   *     GoogleCloudDataplexV1Entity#getName() name}
   * @throws IOException if an IO error occurs or if {@code maxAttempts} is exceeded
   */
  public static GoogleCloudDataplexV1Entity createEntityWithUniqueId(
      DataplexClient dataplex, String zoneName, GoogleCloudDataplexV1Entity entity, int maxAttempts)
      throws IOException {
    String id = entity.getId();
    GoogleCloudDataplexV1Entity entityCopy = entity.clone();

    int attempt = 1;
    while (true) {
      try {
        return dataplex.createEntity(zoneName, entityCopy);
      } catch (GoogleJsonResponseException e) {
        if (errorDetailContains(e, 409, "already exists")) {
          // Assuming the entity ID already exists. Try to re-create it with a new ID.
          if (attempt >= maxAttempts) {
            throw new IOException(
                String.format(
                    "Exceeded maximum attempts (%d) when creating entity (ID = %s) in zone %s",
                    maxAttempts, id, zoneName),
                e);
          }
          attempt++;
          entityCopy = entity.clone().setId(id + "_" + attempt);
          continue;
        }

        throw new IOException(
            String.format("Error creating entity (ID = %s) in zone %s", id, zoneName), e);
      }
    }
  }

  private static boolean errorDetailContains(
      GoogleJsonResponseException e, int statusCode, String message) {
    return e.getStatusCode() == statusCode
        && e.getDetails() != null
        && e.getDetails().getMessage() != null
        && e.getDetails().getMessage().contains(message);
  }
}
