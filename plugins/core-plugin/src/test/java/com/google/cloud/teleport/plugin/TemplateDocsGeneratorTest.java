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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.plugin.model.ImageSpec;
import com.google.cloud.teleport.plugin.model.TemplateDefinitions;
import com.google.cloud.teleport.plugin.sample.AtoBOk;
import freemarker.template.TemplateException;
import java.io.FileWriter;
import java.io.IOException;
import org.junit.Test;

public class TemplateDocsGeneratorTest {

  @Test
  public void testSimpleMarkdownClassic() throws TemplateException, IOException {

    TemplateDefinitions definitions =
        new TemplateDefinitions(AtoBOk.class, AtoBOk.class.getAnnotation(Template.class));
    ImageSpec imageSpec = definitions.buildSpecModel(false);
    imageSpec.getMetadata().setSourceFilePath("README.md");

    String markdown = TemplateDocsGenerator.readmeMarkdown(imageSpec, false);

    FileWriter out =
        new FileWriter(
            "target/README-" + imageSpec.getMetadata().getInternalName() + "-classic.md");
    out.write(markdown);
    out.close();

    // Just check if specific pieces are present
    // We should not gatekeep / slow specific formatting down
    assertThat(markdown).contains("A to B Template");
    assertThat(markdown).contains("Streaming Template that sends A to B");
    assertThat(markdown).contains("inputSubscription");
    assertThat(markdown).contains("gcloud dataflow jobs run");
  }

  @Test
  public void testSimpleMarkdownFlex() throws TemplateException, IOException {

    TemplateDefinitions definitions =
        new TemplateDefinitions(AtoBOk.class, AtoBOk.class.getAnnotation(Template.class));
    ImageSpec imageSpec = definitions.buildSpecModel(false);
    imageSpec.getMetadata().setSourceFilePath("README.md");

    String markdown = TemplateDocsGenerator.readmeMarkdown(imageSpec, true);

    FileWriter out =
        new FileWriter("target/README-" + imageSpec.getMetadata().getInternalName() + "-flex.md");
    out.write(markdown);
    out.close();

    // Just check if specific pieces are present
    // We should not gatekeep / slow specific formatting down
    assertThat(markdown).contains("A to B Template");
    assertThat(markdown).contains("Streaming Template that sends A to B");
    assertThat(markdown).contains("inputSubscription");
    assertThat(markdown).contains("gcloud dataflow flex-template run");
  }
}
