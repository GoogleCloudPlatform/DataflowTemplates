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
package com.google.cloud.syndeo.common;

import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import com.squareup.wire.schema.internal.parser.TypeElement;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;

/**
 * The ProtoSchemaUtils class provides utility methods for converting Protocol Buffers schema
 * definitions into Beam schemas.
 */
public class ProtoSchemaUtils {

  private static final Location DEFAULT_LOCATION = Location.get("");

  /**
   * Converts a Protocol Buffers schema definition into a Beam schema.
   *
   * @param protobufDef the Protocol Buffers schema definition as a string.
   * @return the corresponding Beam schema.
   * @throws SyndeoSchemaParseException if the Protocol Buffers schema is invalid.
   */
  public static Schema beamSchemaFromProtoSchemaDescription(String protobufDef)
      throws SyndeoSchemaParseException {
    ProtoFileElement elm = ProtoParser.Companion.parse(DEFAULT_LOCATION, protobufDef);
    if (elm.getTypes().size() != 1) {
      throw new SyndeoSchemaParseException(
          String.format(
              "Expected a single type defined in schema input, but found %s. "
                  + "Provide only a single top-level proto type to use as the "
                  + "message schema.",
              elm.getTypes().size()));
    }
    TypeElement typeElement = elm.getTypes().stream().findFirst().get();

    if (!(typeElement instanceof MessageElement)) {
      throw new SyndeoSchemaParseException(
          "Expected a message type defined, but found a different type definition for "
              + typeElement.getName());
    }

    return parsedProtoSchemaToBeamSchema(typeElement, null);
  }

  /**
   * Converts a parsed Protocol Buffers schema to a Beam schema.
   *
   * @param typeElement the type element representing the Protocol Buffers schema.
   * @param typeCatalog a catalog of types referenced in the schema.
   * @return the corresponding Beam schema.
   */
  static Schema parsedProtoSchemaToBeamSchema(
      TypeElement typeElement, Map<String, TypeElement> typeCatalog) {
    if (!(typeElement instanceof MessageElement)) {
      throw new IllegalArgumentException("Unsupported type: " + typeElement);
    }
    MessageElement messageElement = (MessageElement) typeElement;
    Map<String, TypeElement> nestedTypes =
        messageElement.getNestedTypes().stream()
            .collect(
                Collectors.<TypeElement, String, TypeElement>toMap(
                    ntype -> ntype.getName(), ntype -> ntype));
    if (typeCatalog != null) {
      nestedTypes.putAll(typeCatalog);
    }

    Schema.Builder typeSchemaBuilder = Schema.builder();

    for (FieldElement f : messageElement.getFields()) {
      if (f.getLabel() == null || f.getLabel().equals(Field.Label.OPTIONAL)) {
        typeSchemaBuilder =
            typeSchemaBuilder.addNullableField(
                f.getName(), protoFieldToBeamFieldType(f, nestedTypes));
      } else {
        typeSchemaBuilder =
            typeSchemaBuilder.addField(f.getName(), protoFieldToBeamFieldType(f, nestedTypes));
      }
    }
    return typeSchemaBuilder.build();
  }

  /**
   * Converts a Protocol Buffers field to a corresponding Beam field type.
   *
   * @param f the Protocol Buffers field element.
   * @param nestedTypes a catalog of types referenced in the schema.
   * @return the corresponding Beam field type.
   */
  static Schema.FieldType protoFieldToBeamFieldType(
      FieldElement f, Map<String, TypeElement> nestedTypes) {
    if (Objects.equals(f.getLabel(), Field.Label.REPEATED)) {
      return Schema.FieldType.array(protoTypeNameToBeamFieldType(f.getType(), nestedTypes));
    } else {
      return protoTypeNameToBeamFieldType(f.getType(), nestedTypes);
    }
  }

  /**
   * Converts a Protobuf field type name to a corresponding Beam schema field type.
   *
   * @param typeName the Protobuf field type name to be converted
   * @param nestedTypes a map containing nested Protobuf types
   * @return the corresponding Beam schema field type
   * @throws IllegalArgumentException if the Protobuf field type is not supported
   */
  static Schema.FieldType protoTypeNameToBeamFieldType(
      String typeName, Map<String, TypeElement> nestedTypes) {
    if (nestedTypes.containsKey(typeName)) {
      if (nestedTypes.get(typeName) instanceof EnumElement) {
        EnumElement enumElm = (EnumElement) nestedTypes.get(typeName);
        return Schema.FieldType.logicalType(
            EnumerationType.create(
                enumElm.getConstants().stream()
                    .collect(
                        Collectors.toMap(
                            enumConstantElm -> enumConstantElm.getName(),
                            enumConstantElement -> enumConstantElement.getTag()))));
      }
      return Schema.FieldType.row(
          parsedProtoSchemaToBeamSchema(nestedTypes.get(typeName), nestedTypes));
    }
    switch (typeName) {
      case "double":
        return Schema.FieldType.DOUBLE;
      case "float":
        return Schema.FieldType.FLOAT;
      case "int32":
        return Schema.FieldType.INT32;
      case "int64":
      case "uint32":
      case "uint64":
      case "sint32":
      case "sint64":
        return Schema.FieldType.INT64;
      case "bool":
        return Schema.FieldType.BOOLEAN;
      case "string":
        return Schema.FieldType.STRING;
      case "bytes":
        return Schema.FieldType.BYTES;
      default:
        throw new IllegalArgumentException(String.format("Unsupported field type: %s", typeName));
    }
  }

  /** An exception thrown when there is an error parsing a Syndeo schema. */
  public static class SyndeoSchemaParseException extends Exception {
    public SyndeoSchemaParseException(String message) {
      super(message);
    }
  }
}
