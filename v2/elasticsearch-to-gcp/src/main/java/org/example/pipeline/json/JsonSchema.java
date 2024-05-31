/*
 * Copyright (C) 2024 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.example.pipeline.json;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Optional;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.StringSchema;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class JsonSchema {

  private static final Logger LOG = LoggerFactory.getLogger(JsonSchema.class);

  public static final String TOP_LEVEL_NAME = "root";
  public static final String TOP_LEVEL_NAMESPACE = "org.apache.beam.sdk.io.gcp.bigquery";
  public static final String DEFAULT_RECORD_DOC_PREFIX = "Translated Avro Schema for ";

  /** How to handle the case on where a Combined property schema is found. */
  public enum CombinedSchemaResolution {
    // in case one of the possible properties is an array type, use it.
    FAVOR_ARRAYS,
    // regardless of any potential container types, use the contained native type
    FAVOR_NATIVE,
    // fails if multiple non-null combined schemas are present for a property, accepted schemas
    // would be (null schema + another).
    FAIL_ON_MULTIPLE;
  }

  public enum MultiArraySchemaResolution {
    DO_NOTHING,
    FLATTEN;
  }

  public record ResolutionConfig(
      CombinedSchemaResolution combinedSchema, MultiArraySchemaResolution arraySchema)
      implements Serializable {
    public static ResolutionConfig of(
        CombinedSchemaResolution combinedSchemaResolution,
        MultiArraySchemaResolution arraySchemaResolution) {
      return new ResolutionConfig(combinedSchemaResolution, arraySchemaResolution);
    }

    public static ResolutionConfig defaultConfig() {
      return ResolutionConfig.of(
          CombinedSchemaResolution.FAIL_ON_MULTIPLE, MultiArraySchemaResolution.DO_NOTHING);
    }
  }

  public static Schema avroSchemaFromJsonSchema(String jsonSchemaStr) {
    return avroSchemaFromJsonSchema(jsonSchemaStr, ResolutionConfig.defaultConfig());
  }

  public static Schema avroSchemaFromJsonSchema(String jsonSchemaStr, ResolutionConfig resolution) {
    return avroSchemaFromJsonSchema(jsonSchemaFromString(jsonSchemaStr), null, null, resolution);
  }

  private static ObjectSchema jsonSchemaFromString(String jsonSchema) {
    JSONObject parsedSchema = new JSONObject(jsonSchema);
    org.everit.json.schema.Schema schemaValidator =
        org.everit.json.schema.loader.SchemaLoader.load(parsedSchema);
    if (!(schemaValidator instanceof ObjectSchema)) {
      throw new IllegalArgumentException(
          String.format("The schema is not a valid object schema:%n %s", jsonSchema));
    }
    return (org.everit.json.schema.ObjectSchema) schemaValidator;
  }

  static Schema avroSchemaFromJsonSchema(
      ObjectSchema jsonSchema, String recordName, String parent, ResolutionConfig resolution) {
    var actualRecordName = Optional.ofNullable(recordName).orElse(TOP_LEVEL_NAME);
    var recordNamespace =
        Optional.ofNullable(parent)
            .map(prnt -> TOP_LEVEL_NAMESPACE + "." + prnt)
            .orElse(TOP_LEVEL_NAMESPACE);
    var recordPropertyNamespace =
        Optional.ofNullable(parent)
            .map(prnt -> prnt + "." + actualRecordName)
            .orElse(actualRecordName);
    var avroSchemaBuilder =
        SchemaBuilder.record(actualRecordName)
            .namespace(recordNamespace)
            .doc(DEFAULT_RECORD_DOC_PREFIX + actualRecordName)
            .fields();
    var properties =
        new HashMap<String, org.everit.json.schema.Schema>(jsonSchema.getPropertySchemas());
    if (properties.isEmpty()) {
      LOG.warn(
          String.format(
              "Detected empty object schema (%s), "
                  + "defaulting to Avro string type for the field %s.%s.",
              jsonSchema, parent, recordName));
      return SchemaBuilder.builder().stringType();
    }
    // Properties in a JSON Schema are stored in a Map object and unfortunately don't maintain
    // order. However, the schema's required properties is a list of property names that is
    // consistent and is in the same order as when the schema was first created. To create a
    // consistent Beam Schema from the same JSON schema, we add Schema Fields following this order.
    // We can guarantee a consistent Beam schema when all JSON properties are required.
    for (var propertyName : jsonSchema.getRequiredProperties()) {
      var propertySchema = properties.get(propertyName);
      if (propertySchema == null) {
        throw new IllegalArgumentException("Unable to parse schema " + jsonSchema);
      }

      Boolean isNullable =
          Boolean.TRUE.equals(propertySchema.getUnprocessedProperties().get("nullable"));
      avroSchemaBuilder =
          addPropertySchemaToRecordSchema(
              propertyName,
              recordPropertyNamespace,
              propertySchema,
              avroSchemaBuilder,
              isNullable,
              resolution);
      // Remove properties we already added.
      properties.remove(propertyName, propertySchema);
    }

    // Now we are potentially left with properties that are not required. Add them too.
    // Note: having more than one non-required properties may result in  inconsistent
    // Beam schema field orderings.
    for (var entry : properties.entrySet()) {
      String propertyName = entry.getKey();
      org.everit.json.schema.Schema propertySchema = entry.getValue();

      if (propertySchema == null) {
        throw new IllegalArgumentException("Unable to parse schema " + jsonSchema);
      }
      // Consider non-required properties to be nullable
      avroSchemaBuilder =
          addPropertySchemaToRecordSchema(
              propertyName,
              recordPropertyNamespace,
              propertySchema,
              avroSchemaBuilder,
              true,
              resolution);
    }

    return avroSchemaBuilder.endRecord();
  }

  private static SchemaBuilder.FieldAssembler<Schema> addPropertySchemaToRecordSchema(
      String propertyName,
      String parentName,
      org.everit.json.schema.Schema propertySchema,
      SchemaBuilder.FieldAssembler<Schema> recordFieldsAssembler,
      Boolean isNullable,
      ResolutionConfig resolution) {
    if (propertySchema instanceof ArraySchema arraySchema) {
      var baseType = recordFieldsAssembler.name(propertyName);
      var elementSchema =
          avroSchemaFromJsonSchemaType(
              arraySchema, propertyName + ".elements", parentName, false, resolution);
      var arrayDefault = baseType.type(elementSchema);
      // no default support for now
      return arrayDefault.noDefault();
    } else {
      try {
        var fieldSchema =
            avroSchemaFromJsonSchemaType(
                propertySchema, propertyName, parentName, isNullable, resolution);
        var baseField = recordFieldsAssembler.name(propertyName);
        // no default support for now
        return baseField.type(fieldSchema).noDefault();
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Unsupported field type " + propertySchema.getClass() + " in field " + propertyName, e);
      }
    }
  }

  private static Schema avroSchemaFromJsonSchemaType(
      org.everit.json.schema.Schema propertySchema,
      String propertyName,
      String parentName,
      boolean isNullable,
      ResolutionConfig resolution) {
    var builder = SchemaBuilder.builder();
    return switch (propertySchema) {
      case ObjectSchema objSchema -> isNullable
          ? SchemaBuilder.unionOf()
              .nullType()
              .and()
              .type(
                  builder.type(
                      avroSchemaFromJsonSchema(objSchema, propertyName, parentName, resolution)))
              .endUnion()
          : builder.type(avroSchemaFromJsonSchema(objSchema, propertyName, parentName, resolution));
      case BooleanSchema __ -> isNullable
          ? SchemaBuilder.unionOf().nullType().and().type(builder.booleanType()).endUnion()
          : builder.booleanType();
      case NumberSchema numSchema -> {
        var avroNumSchema =
            numSchema.requiresInteger()
                ? builder.longType()
                : builder.type(
                    LogicalTypes.decimal(77, 38).addToSchema(Schema.create(Schema.Type.BYTES)));
        yield isNullable
            ? SchemaBuilder.unionOf().nullType().and().type(avroNumSchema).endUnion()
            : avroNumSchema;
      }
      case StringSchema __ -> isNullable
          ? SchemaBuilder.unionOf().nullType().and().type(builder.stringType()).endUnion()
          : builder.stringType();
      case ReferenceSchema refSchema -> avroSchemaFromJsonSchemaType(
          refSchema.getReferredSchema(), propertyName, parentName, isNullable, resolution);
      case ArraySchema arraySchema -> Optional.ofNullable(arraySchema.getAllItemSchema())
          .map(
              elementSchema ->
                  switch (resolution.arraySchema()) {
                    case FLATTEN -> switch (elementSchema) {
                      case CombinedSchema combSchema -> firstNonNullableSchema(combSchema)
                          .filter(innerSchema -> innerSchema instanceof ArraySchema)
                          .map(
                              firstArraySchema ->
                                  avroSchemaFromJsonSchemaType(
                                      firstArraySchema,
                                      propertyName,
                                      parentName,
                                      isNullable,
                                      resolution))
                          .orElse(
                              avroSchemaFromJsonSchemaType(
                                  elementSchema, propertyName, parentName, isNullable, resolution));
                      case ArraySchema innerArraySchema -> avroSchemaFromJsonSchemaType(
                          innerArraySchema, propertyName, parentName, isNullable, resolution);
                      default -> builder
                          .array()
                          .items(
                              avroSchemaFromJsonSchemaType(
                                  elementSchema, propertyName, parentName, false, resolution));
                    };
                    case DO_NOTHING -> builder
                        .array()
                        .items(
                            avroSchemaFromJsonSchemaType(
                                elementSchema, propertyName, parentName, isNullable, resolution));
                  })
          .orElseGet(
              () -> {
                var maybeItemSchemas =
                    Optional.ofNullable(arraySchema.getItemSchemas())
                        .filter(itemSchemas -> !itemSchemas.isEmpty());
                if (maybeItemSchemas.isPresent()) {
                  throw new IllegalArgumentException(
                      String.format(
                          "Array schema is not properly formatted or unsupported (%s). Note that "
                              + "JSON-schema's tuple-like arrays are not supported by Beam.",
                          propertySchema));
                }
                return builder.array().items().stringType();
              });
      case CombinedSchema combSchema -> switch (resolution.combinedSchema()) {
        case FAIL_ON_MULTIPLE -> validateAndProcessMultipleCombinedSchema(
            combSchema, propertyName, parentName, isNullable, resolution);
        case FAVOR_ARRAYS -> processCombinedSchemaFavorArrayType(
            combSchema, propertyName, parentName, isNullable, resolution);
        case FAVOR_NATIVE -> processCombinedSchemaFavorNativeType(
            combSchema, propertyName, parentName, isNullable, resolution);
      };
      case NullSchema __ -> builder.unionOf().nullType().and().stringType().endUnion();
      default -> throw new IllegalArgumentException(
          "Unsupported schema type: " + propertySchema.getClass());
    };
  }

  private static Schema validateAndProcessMultipleCombinedSchema(
      CombinedSchema combSchema,
      String propertyName,
      String parentName,
      boolean isNullable,
      ResolutionConfig resolution) {
    // we support only null + other simple schema here, if this definition has more, then we
    // fail the translation
    if (combSchema.getSubschemas().size() > 2) {
      throw new IllegalArgumentException(
          "The provided JSON schema has more than one option for the field "
              + "(besides the null option) and is not supported: "
              + combSchema.toString());
    }
    var hasNullSchema =
        combSchema.getSubschemas().stream()
                .filter(subSchema -> subSchema instanceof NullSchema)
                .count()
            == 1;
    if (!hasNullSchema) {
      throw new IllegalArgumentException(
          "The provided JSON schema does not have a null schema as part of the combined schema,"
              + " that's not supported: "
              + combSchema.toString());
    }
    return avroSchemaFromJsonSchemaType(
        combSchema.getSubschemas().stream()
            .filter(subSchema -> !(subSchema instanceof NullSchema))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "The provided JSON schema should have at least a non null schema as "
                            + "part of the combined schema: "
                            + combSchema.toString())),
        propertyName,
        parentName,
        isNullable,
        resolution);
  }

  private static Schema processCombinedSchemaFavorArrayType(
      CombinedSchema combSchema,
      String propertyName,
      String parentName,
      boolean isNullable,
      ResolutionConfig resolution) {
    return validateAndProcessCombinedWithCondition(
        combSchema,
        propertyName,
        parentName,
        isNullable,
        resolution,
        true,
        "The provided JSON schema should have an array type schema besides"
            + " a non null schema as part of the combined schema: ");
  }

  private static Schema processCombinedSchemaFavorNativeType(
      CombinedSchema combSchema,
      String propertyName,
      String parentName,
      boolean isNullable,
      ResolutionConfig resolution) {
    return validateAndProcessCombinedWithCondition(
        combSchema,
        propertyName,
        parentName,
        isNullable,
        resolution,
        false,
        "The provided JSON schema should have an basic type schema besides"
            + " a non null schema as part of the combined schema: ");
  }

  private static Optional<org.everit.json.schema.Schema> firstNonNullableSchema(
      CombinedSchema combinedSchema) {
    return combinedSchema.getSubschemas().stream()
        .filter(subSchema -> !(subSchema instanceof NullSchema))
        .findFirst();
  }

  private static Schema validateAndProcessCombinedWithCondition(
      CombinedSchema combSchema,
      String propertyName,
      String parentName,
      boolean isNullable,
      ResolutionConfig resolution,
      boolean shouldFavorArray,
      String errorMessage) {
    var firstNonNullableSchema = firstNonNullableSchema(combSchema);

    return avroSchemaFromJsonSchemaType(
        combSchema.getSubschemas().stream()
            .filter(subSchema -> !(subSchema instanceof NullSchema))
            // honor requested type preference
            .filter(
                subSchema ->
                    shouldFavorArray
                        ? subSchema instanceof ArraySchema
                        : !(subSchema instanceof ArraySchema))
            .findFirst()
            // if empty, then provide first non nullable, otherwise fail.
            .orElse(
                firstNonNullableSchema.orElseThrow(
                    () -> new IllegalArgumentException(errorMessage + combSchema.toString()))),
        propertyName,
        parentName,
        isNullable,
        resolution);
  }
}
