/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.transformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.commons.io.IOUtils;

/** Coder Class for {@link JsonNode} class. */
public class JsonNodeCoder extends CustomCoder<JsonNode> {

  public static JsonNodeCoder of() {
    return new JsonNodeCoder();
  }

  @Override
  public void encode(JsonNode node, OutputStream outStream) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    String nodeString = mapper.writeValueAsString(node);
    outStream.write(nodeString.getBytes());
  }

  @Override
  public JsonNode decode(InputStream inStream) throws IOException {
    byte[] bytes = IOUtils.toByteArray(inStream);
    ObjectMapper mapper = new ObjectMapper();
    String json = new String(bytes);
    return mapper.readTree(json);
  }
}
