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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CharsetMapperTest {

  @Test
  public void testToJavaCharset_StandardMappings() {
    assertThat(CharsetMapper.toJavaCharset("utf8mb4")).hasValue(StandardCharsets.UTF_8);
    assertThat(CharsetMapper.toJavaCharset("utf8")).hasValue(StandardCharsets.UTF_8);
    assertThat(CharsetMapper.toJavaCharset("latin1")).hasValue(StandardCharsets.ISO_8859_1);
    assertThat(CharsetMapper.toJavaCharset("ascii")).hasValue(StandardCharsets.US_ASCII);
    assertThat(CharsetMapper.toJavaCharset("binary")).hasValue(StandardCharsets.ISO_8859_1);
    assertThat(CharsetMapper.toJavaCharset("sql_ascii")).hasValue(StandardCharsets.US_ASCII);
  }

  @Test
  public void testToJavaCharset_NormalizationAndCasing() {
    assertThat(CharsetMapper.toJavaCharset("  UTF-8  ")).hasValue(StandardCharsets.UTF_8);
    assertThat(CharsetMapper.toJavaCharset("UTF_8")).hasValue(StandardCharsets.UTF_8);
    assertThat(CharsetMapper.toJavaCharset("LATIN1")).hasValue(StandardCharsets.ISO_8859_1);
    assertThat(CharsetMapper.toJavaCharset("iso_8859_1")).hasValue(StandardCharsets.ISO_8859_1);
  }

  @Test
  public void testToJavaCharset_Unsupported() {
    assertThat(CharsetMapper.toJavaCharset("invalid_charset")).isEmpty();
    assertThat(CharsetMapper.toJavaCharset(null)).isEmpty();
    assertThat(CharsetMapper.toJavaCharset("")).isEmpty();
  }
}
