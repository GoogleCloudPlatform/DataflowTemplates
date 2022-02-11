/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.elasticsearch.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ProcessValidateJsonFields is used to fix malformed fields, (ex. de-dot). */
public class ProcessValidateJsonFields
    extends PTransform<PCollection<String>, PCollection<String>> {

  @Override
  public PCollection<String> expand(PCollection<String> input) {
    return input.apply(ParDo.of(new ValidateJsonFieldsFn()));
  }

  static class ValidateJsonFieldsFn extends DoFn<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateJsonFieldsFn.class);
    ObjectMapper objectMapper = new ObjectMapper();

    private static final Configuration configuration =
        Configuration.builder()
            .jsonProvider(new JacksonJsonNodeJsonProvider())
            .mappingProvider(new JacksonMappingProvider())
            .build();

    /**
     * "Having a field name with just dot will cause Elasticsearch to throw exception Index -1 out
     * of bounds for length 0. This is because field like ".": {} is not valid. The error is thrown
     * on this line because subfields has a length of 0 and hence it is trying to access index -1,
     * which is not allowed. The method that splits the field names into paths called
     * splitAndValidatePath and what it does is to split field names when it encounters . (i.e. a
     * dot). This field is easily found in k8s logs"
     *
     * @param context
     */
    @ProcessElement
    public void processElement(ProcessContext context) {
      String input = context.element();
      ObjectNode node = null;
      try {
        JsonPath.using(configuration)
            .parse(input)
            .read(
                "$.protoPayload.response.spec.template.spec.containers.livenessProbe.httpGet.port");
      } catch (Exception ignored) {
      }

      JsonNode requestApp = null;
      try {
        requestApp =
            JsonPath.using(configuration)
                .parse(input)
                .read("$.protoPayload.request.metadata.labels.app");
        if (requestApp != null) {
          input =
              JsonPath.using(configuration)
                  .parse(input)
                  .put(
                      "$.protoPayload.request.metadata.labels",
                      "app.kubernetes.io/name",
                      JsonPath.using(configuration)
                          .parse(input)
                          .read("$.protoPayload.request.metadata.labels.app"))
                  .jsonString();
          input =
              JsonPath.using(configuration)
                  .parse(input)
                  .delete("$.protoPayload.request.metadata.labels.app")
                  .jsonString();
        }
      } catch (Exception ignored) {
      }

      JsonNode responseApp = null;
      try {
        responseApp =
            JsonPath.using(configuration)
                .parse(input)
                .read("$.protoPayload.response.metadata.labels.app");

        if (responseApp != null) {
          input =
              JsonPath.using(configuration)
                  .parse(input)
                  .put(
                      "$.protoPayload.response.metadata.labels",
                      "app.kubernetes.io/name",
                      JsonPath.using(configuration)
                          .parse(input)
                          .read("$.protoPayload.response.metadata.labels.app"))
                  .jsonString();
          input =
              JsonPath.using(configuration)
                  .parse(input)
                  .delete("$.protoPayload.response.metadata.labels.app")
                  .jsonString();
        }
      } catch (Exception ignored) {
      }

      try {
        try {
          JsonNode response = node.get("protoPayload").get("response");
          input =
              JsonPath.using(configuration)
                  .parse(input)
                  .put("$.protoPayload.response", "status", objectMapper.createObjectNode())
                  .jsonString();
        } catch (NullPointerException ex) {
        }

        node = removeEmptyFields((ObjectNode) objectMapper.readTree(input));
      } catch (JsonProcessingException e) {
        LOG.warn("Unable to parse Json payload. " + e);
      }

      try {
        JsonPath.using(configuration).parse(input).read("$.protoPayload.response.status");
      } catch (Exception e) {
        node =
            JsonPath.using(configuration)
                .parse(node.toString())
                .put("$.protoPayload.response", "status", objectMapper.createObjectNode())
                .json();
      }

      context.output(node.toString());
    }

    public static ObjectNode removeEmptyFields(final ObjectNode jsonNode) {
      ObjectNode ret = new ObjectMapper().createObjectNode();
      Iterator<Map.Entry<String, JsonNode>> iter = jsonNode.fields();

      while (iter.hasNext()) {
        Map.Entry<String, JsonNode> entry = iter.next();
        String key = entry.getKey();
        JsonNode value = entry.getValue();

        if (key.equals(".")) {
          ret.set("_", value);
          continue;
        }

        if (value instanceof ObjectNode) {
          Map<String, ObjectNode> map = new HashMap<>();
          map.put(key, removeEmptyFields((ObjectNode) value));
          ret.setAll(map);
        } else if (value instanceof ArrayNode) {
          ret.set(key, removeEmptyFields((ArrayNode) value));
        } else if (value.asText() != null) {
          ret.set(key, value);
        }
      }

      return ret;
    }

    public static ArrayNode removeEmptyFields(ArrayNode array) {
      ArrayNode ret = new ObjectMapper().createArrayNode();
      Iterator<JsonNode> iter = array.elements();

      while (iter.hasNext()) {
        JsonNode value = iter.next();

        if (value instanceof ArrayNode) {
          ret.add(removeEmptyFields((ArrayNode) (value)));
        } else if (value instanceof ObjectNode) {
          ret.add(removeEmptyFields((ObjectNode) (value)));
        } else if (value != null) {
          ret.add(value);
        }
      }

      return ret;
    }
  }
}
