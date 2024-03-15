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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Test;

/** Unit tests for {@link com.google.cloud.teleport.v2.transformer.JsonNodeCoder} class. */
public class JsonNodeCoderTest {

  /**
   * Test whether {@link JsonNodeCoder} is able to encode/decode a {@link JsonNode} correctly.
   *
   * @throws IOException
   */
  @Test
  public void jsonNodeCoderTestBasic() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode originalEvent = mapper.readTree("{\"col1\":1, \"col2\": \"value\"}");

    JsonNodeCoder coder = JsonNodeCoder.of();
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      coder.encode(originalEvent, bos);
      try (ByteArrayInputStream bin = new ByteArrayInputStream(bos.toByteArray())) {
        JsonNode decodedEvent = coder.decode(bin);
        assertThat(decodedEvent, is(equalTo(originalEvent)));
      }
    }
  }
}
