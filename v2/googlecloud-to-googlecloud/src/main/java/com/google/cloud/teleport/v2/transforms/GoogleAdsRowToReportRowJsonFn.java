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

import com.google.ads.googleads.lib.utils.FieldMasks;
import com.google.ads.googleads.v14.services.GoogleAdsRow;
import com.google.cloud.teleport.v2.utils.GoogleAdsUtils;
import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * This transform lifts {@link GoogleAdsRow} nested fields selected fields in the <a
 * href="https://developers.google.com/google-ads/api/docs/query/overview">Google Ads Query Language
 * query</a> up as top-level fields, replacing the path separator with an underscore, and formats
 * the result as JSON.
 */
public class GoogleAdsRowToReportRowJsonFn extends DoFn<GoogleAdsRow, String> {
  private static final Gson GSON = new Gson();
  private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();

  private final String query;
  private transient Map<String, Descriptors.FieldDescriptor> selectedFields;

  public GoogleAdsRowToReportRowJsonFn(String query) {
    this.query = query;
  }

  @Setup
  public void setup() {
    selectedFields = GoogleAdsUtils.getSelectedFieldDescriptors(query);
  }

  private void printField(
      JsonWriter jsonWriter, Descriptors.FieldDescriptor.JavaType javaType, Object value)
      throws IOException {
    switch (javaType) {
      case INT:
        jsonWriter.value((Integer) value);
        return;
      case LONG:
        jsonWriter.value((Long) value);
        return;
      case FLOAT:
        jsonWriter.value((Float) value);
        return;
      case DOUBLE:
        jsonWriter.value((Double) value);
        return;
      case BOOLEAN:
        jsonWriter.value((Boolean) value);
        return;
      case STRING:
        jsonWriter.value((String) value);
        return;
      case BYTE_STRING:
        jsonWriter.value(BaseEncoding.base64().encode(((ByteString) value).toByteArray()));
        return;
      case ENUM:
        jsonWriter.value(((Descriptors.EnumValueDescriptor) value).getName());
        return;
      case MESSAGE:
        jsonWriter.jsonValue(JSON_PRINTER.print((MessageOrBuilder) value));
        return;
      default:
        throw new IllegalArgumentException(String.format("Unrecognized type: %s", javaType));
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws IOException {
    GoogleAdsRow row = c.element();
    StringWriter writer = new StringWriter();
    JsonWriter jsonWriter = GSON.newJsonWriter(writer);

    jsonWriter.beginObject();
    for (Map.Entry<String, Descriptors.FieldDescriptor> entry : selectedFields.entrySet()) {
      String fieldPath = entry.getKey();
      Descriptors.FieldDescriptor fieldDescriptor = entry.getValue();

      jsonWriter.name(fieldPath.replace('.', '_'));
      List<?> values = FieldMasks.getFieldValue(fieldPath, row);

      if (values.size() == 0) {
        jsonWriter.nullValue();
        break;
      }

      if (fieldDescriptor.isRepeated()) {
        jsonWriter.beginArray();
        for (Object value : values) {
          printField(jsonWriter, fieldDescriptor.getJavaType(), value);
        }
        jsonWriter.endArray();
      } else {
        printField(jsonWriter, fieldDescriptor.getJavaType(), values.get(0));
      }
    }
    jsonWriter.endObject();

    c.output(writer.toString());
  }
}
