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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper;

import static com.google.common.truth.Truth.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CodepointGeneratorTest {

  @Test
  public void testGetValidCodepoints_LargeCharsetsAborts() {
    assertThat(CodepointGenerator.getValidCodepoints(StandardCharsets.UTF_8, 1000)).isEmpty();
    assertThat(CodepointGenerator.getValidCodepoints(StandardCharsets.UTF_16, 1000)).isEmpty();
  }

  @Test
  public void testGetValidCodepoints_Ascii() {
    Optional<List<Integer>> codepointsOpt =
        CodepointGenerator.getValidCodepoints(StandardCharsets.US_ASCII, 200);
    assertThat(codepointsOpt).isPresent();
    List<Integer> codepoints = codepointsOpt.get();
    // ASCII has 128 valid single-byte characters
    assertThat(codepoints.size()).isEqualTo(128);
    // Verify they are in range U+0000 to U+007F
    for (int i = 0; i < 128; i++) {
      assertThat(codepoints.get(i)).isEqualTo(i);
    }
  }

  @Test
  public void testGetValidCodepoints_Latin1() {
    Optional<List<Integer>> codepointsOpt =
        CodepointGenerator.getValidCodepoints(StandardCharsets.ISO_8859_1, 300);
    assertThat(codepointsOpt).isPresent();
    List<Integer> codepoints = codepointsOpt.get();
    // ISO-8859-1 has 256 valid single-byte characters
    assertThat(codepoints.size()).isEqualTo(256);
    // Verify they are in range U+0000 to U+00FF
    for (int i = 0; i < 256; i++) {
      assertThat(codepoints.get(i)).isEqualTo(i);
    }
  }

  @Test
  public void testGetValidCodepoints_ExceedLimit() {
    // US_ASCII has 128 valid characters. If we limit to 100, it should return empty.
    Optional<List<Integer>> codepointsOpt =
        CodepointGenerator.getValidCodepoints(StandardCharsets.US_ASCII, 100);
    assertThat(codepointsOpt).isEmpty();
  }
}
