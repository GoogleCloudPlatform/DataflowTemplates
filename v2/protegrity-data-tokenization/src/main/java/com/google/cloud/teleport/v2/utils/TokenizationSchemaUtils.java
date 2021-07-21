/*
 * Copyright (C) 2020 Google Inc.
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
package com.google.cloud.teleport.v2.utils;

import static com.google.cloud.teleport.v2.utils.SchemaUtils.getGcsFileAsString;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.Gson;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@link TokenizationSchemaUtils} Class to read JSON based schema. Is there available to read from file or
 * from string. Currently supported local File System and GCS.
 */
public class TokenizationSchemaUtils {

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(TokenizationSchemaUtils.class);


  public static Map<String, String> getDataElementsToTokenize(String payloadConfigGcsPath,
      Schema beamSchema) {
    Map<String, String> dataElements;
    try {
      String rawJsonWithDataElements = getGcsFileAsString(payloadConfigGcsPath);
      Gson gson = new Gson();
      Type type = new TypeToken<HashMap<String, String>>() {
      }.getType();
      dataElements = gson
          .fromJson(rawJsonWithDataElements, type);
    } catch (NullPointerException exception) {
      LOG.error(
          "Cant parse fields to tokenize, or input parameter payloadConfigGcsPath was not specified."
              + " All fields will be sent to the protectors");
      dataElements = beamSchema.getFields().stream().collect(
          Collectors.toMap(Field::getName, e -> ""));
    }
    return dataElements;
  }

}
