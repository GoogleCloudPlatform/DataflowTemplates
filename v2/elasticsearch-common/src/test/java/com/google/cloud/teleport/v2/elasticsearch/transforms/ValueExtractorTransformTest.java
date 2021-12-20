/*
 * Copyright (C) 2019 Google LLC
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

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import java.io.IOException;
import org.junit.Test;

/** Tests for the {@link ValueExtractorTransform} transform. */
public class ValueExtractorTransformTest {

  private static final String RESOURCES_DIR = "ValueExtractorTransformTest/";

  private static final String TRANSFORM_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "transform.js").getPath();

  /** Tests that {@link ValueExtractorTransform.ValueExtractorFn} returns the correct value. */
  @Test
  public void testValueExtractorFn() throws IOException {

    final String stringifiedJsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";

    final String jsonRecord =
        "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23,\"someProp\":\"someValue\"}";

    ObjectMapper objectMapper = new ObjectMapper();

    JsonNode jsonNode = objectMapper.readTree(stringifiedJsonRecord);

    String result =
        ValueExtractorTransform.ValueExtractorFn.newBuilder()
            .setFileSystemPath(TRANSFORM_FILE_PATH)
            .setFunctionName("transform")
            .build()
            .apply(jsonNode);

    assertThat(result, is(jsonRecord));
  }

  /**
   * Tests that {@link ValueExtractorTransform.ValueExtractorFn} returns null when both inputs are
   * null.
   */
  @Test
  public void testValueExtractorFnReturnNull() throws IOException {

    final String stringifiedJsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";

    final String jsonRecord =
        "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23,\"someProp\":\"someValue\"}";

    ObjectMapper objectMapper = new ObjectMapper();

    JsonNode jsonNode = objectMapper.readTree(stringifiedJsonRecord);

    String result =
        ValueExtractorTransform.ValueExtractorFn.newBuilder()
            .setFileSystemPath(null)
            .setFunctionName(null)
            .build()
            .apply(jsonNode);

    assertThat(result, is(nullValue()));
  }

  /**
   * Tests that {@link ValueExtractorTransform.ValueExtractorFn} throws exception if only {@link
   * ValueExtractorTransform.ValueExtractorFn#fileSystemPath()} is supplied.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValueExtractorFnNullSystemPath() throws IOException {

    final String stringifiedJsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";

    final String jsonRecord =
        "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23,\"someProp\":\"someValue\"}";

    ObjectMapper objectMapper = new ObjectMapper();

    JsonNode jsonNode = objectMapper.readTree(stringifiedJsonRecord);

    String result =
        ValueExtractorTransform.ValueExtractorFn.newBuilder()
            .setFileSystemPath(null)
            .setFunctionName("transform")
            .build()
            .apply(jsonNode);
  }

  /**
   * Tests that {@link ValueExtractorTransform.ValueExtractorFn} throws exception if only {@link
   * ValueExtractorTransform.ValueExtractorFn#functionName()} is supplied.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValueExtractorFnNullFunctionName() throws IOException {

    final String stringifiedJsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";

    final String jsonRecord =
        "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23,\"someProp\":\"someValue\"}";

    ObjectMapper objectMapper = new ObjectMapper();

    JsonNode jsonNode = objectMapper.readTree(stringifiedJsonRecord);

    String result =
        ValueExtractorTransform.ValueExtractorFn.newBuilder()
            .setFileSystemPath(TRANSFORM_FILE_PATH)
            .setFunctionName(null)
            .build()
            .apply(jsonNode);
  }
}
