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
package com.google.cloud.teleport.plugin;

import static com.google.cloud.teleport.plugin.TemplateDefinitionsParser.parseVersion;
import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

/** Test for {@link TemplateDefinitionsParser} class. */
public class TemplateDefinitionsParserTest {

  @Test
  public void testParseVersionRegular() {
    assertThat(parseVersion("2023-03-10_RC00-00")).isEqualTo("2023-03-10_rc00-00");
  }

  @Test
  public void testParseVersionStrippingBadChars() {
    assertThat(parseVersion("2023-03-10_RC00-00(patch1)")).isEqualTo("2023-03-10_rc00-00_patch1_");
    assertThat(parseVersion("2023_03_10_RC00-00[{#$")).isEqualTo("2023_03_10_rc00-00____");
  }

  @Test
  public void testParseVersionWithPath() {
    assertThat(parseVersion("templates/2023-03-10_RC00-00")).isEqualTo("2023-03-10_rc00-00");
  }

  @Test
  public void testParseVersionWithRootPath() {
    assertThat(parseVersion("/templates/2023-03-10_RC00-00")).isEqualTo("2023-03-10_rc00-00");
  }
}
