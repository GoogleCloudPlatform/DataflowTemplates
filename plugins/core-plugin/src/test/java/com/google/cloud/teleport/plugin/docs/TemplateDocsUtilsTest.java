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
package com.google.cloud.teleport.plugin.docs;

import static com.google.cloud.teleport.plugin.docs.TemplateDocsUtils.replaceSiteTags;
import static com.google.cloud.teleport.plugin.docs.TemplateDocsUtils.replaceVariableInterpolationNames;
import static com.google.cloud.teleport.plugin.docs.TemplateDocsUtils.wrapText;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

/** Tests for {@link TemplateDocsUtils}. */
public class TemplateDocsUtilsTest {

  @Test
  public void testReplaceVariableInterpolationNamesNone() {
    assertEquals(
        "This is a template that sends data to Inexistent Product",
        replaceVariableInterpolationNames(
            "This is a template that sends data to Inexistent Product"));
  }

  @Test
  public void testReplaceVariableInterpolationNamesSingle() {
    assertEquals(
        "This is a template that sends data to {{product_name_elasticsearch}}",
        replaceVariableInterpolationNames("This is a template that sends data to Elasticsearch"));
  }

  @Test
  public void testReplaceVariableInterpolationNamesDouble() {
    assertEquals(
        "{{gcp_name_short}} {{dataflow_name}}",
        replaceVariableInterpolationNames("Google Cloud Dataflow"));
  }

  @Test
  public void testWrapText() {
    assertEquals("short line", wrapText("short line", 16, null, false));
    assertEquals("somewhat longer\nline", wrapText("somewhat longer line", 16, null, false));
  }

  @Test
  public void testWrapTextListModeNoText() {
    assertEquals(
        "short line\n"
            + "<ul>\n"
            + "  <li>bzip</li>\n"
            + "  <li>gzip</li>\n"
            + "  <li>tar</li>\n"
            + "</ul>",
        wrapText("short line\n" + "- bzip\n" + "- gzip\n" + "- tar", 16, null, true));
  }

  @Test
  public void testWrapTextListModePostText() {
    assertEquals(
        "short line\n"
            + "<ul>\n"
            + "  <li>bzip</li>\n"
            + "  <li>gzip</li>\n"
            + "  <li>tar</li>\n"
            + "</ul>\n"
            + "ended",
        wrapText("short line\n" + "- bzip\n" + "- gzip\n" + "- tar\n" + "ended", 16, null, true));
  }

  @Test
  public void testWrapSiteList() {
    assertEquals(
        "The schema of the destination table is inferred from the source Cassandra table.\n"
            + "<ul>\n"
            + "  <li><code>List</code> and `Set` will be mapped to BigQuery `REPEATED` fields.</li>\n"
            + "  <li>`Map` will be mapped to BigQuery `RECORD` fields.</li>\n"
            + "  <li>All other types are mapped to BigQuery fields with the corresponding types.</li>\n"
            + "  <li>Cassandra user-defined types (UDTs) and tuple data types are not supported.</li>\n"
            + "</ul>",
        wrapText(
            "The schema of the destination table is inferred from the source Cassandra table.\n"
                + "- <code>List</code> and `Set` will be mapped to BigQuery `REPEATED` fields.\n"
                + "- `Map` will be mapped to BigQuery `RECORD` fields.\n"
                + "- All other types are mapped to BigQuery fields with the corresponding types.\n"
                + "- Cassandra user-defined types (UDTs) and tuple data types are not supported.",
            80,
            null,
            true));
  }

  @Test
  public void testWrapTextExistingBreak() {
    assertEquals("short\nline", wrapText("short\nline", 16, null, false));
  }

  @Test
  public void testWrapTextPrepad() {
    assertEquals("somewhat longer\n  line", wrapText("somewhat longer line", 16, "  ", false));
  }

  @Test
  public void testReplaceSiteTags() {
    assertEquals(
        "This has the project id. For example, <code>project_id</code>.",
        replaceSiteTags("This has the project id. (Example: project_id)"));
  }
}
