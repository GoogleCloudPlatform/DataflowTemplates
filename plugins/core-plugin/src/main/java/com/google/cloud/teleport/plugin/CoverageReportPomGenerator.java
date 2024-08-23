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
package com.google.cloud.teleport.plugin;

import com.google.common.base.Strings;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

public class CoverageReportPomGenerator {
  private static final Logger LOG = Logger.getLogger(CoverageReportPomGenerator.class.getName());

  private static final List<String> MODULES_TO_SKIP = List.of("report-coverage", "it");
  private static final String GROUP_ID = "groupId";
  private static final String ARTIFACT_ID = "artifactId";

  private CoverageReportPomGenerator() {}

  public static void generateCoverageReportPom(String rootDir)
      throws IOException, TemplateException {
    List<List<String>> entries = new ArrayList<>();
    findPomFiles(new File(rootDir), entries);

    StringBuilder dependencies = new StringBuilder();
    entries.forEach(
        entry -> dependencies.append(createDependencyBlock(entry.get(0), entry.get(1))));

    Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_32);
    freemarkerConfig.setDefaultEncoding("UTF-8");
    freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    freemarkerConfig.setLogTemplateExceptions(true);
    freemarkerConfig.setClassForTemplateLoading(CoverageReportPomGenerator.class, "/");

    Map<String, String> parameters = Map.of("dependencies", dependencies.toString());

    Template template = freemarkerConfig.getTemplate("pom-template");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStreamWriter writer = new OutputStreamWriter(baos);

    try {
      template.process(parameters, writer);
      writer.flush();

      Files.write(
          Path.of(rootDir, "report-coverage/pom.xml"),
          baos.toString(StandardCharsets.UTF_8).getBytes());
    } catch (Exception e) {
      LOG.warning("Unable to generate pom.xml");
      throw e;
    }
  }

  private static String createDependencyBlock(String groupId, String artifactId) {
    return String.format(
        "        <dependency>\n"
            + "            <groupId>%s</groupId>\n"
            + "            <artifactId>%s</artifactId>\n"
            + "            <version>${project.version}</version>\n"
            + "        </dependency>\n",
        groupId, artifactId);
  }

  private static void findPomFiles(File directory, List<List<String>> entries) {
    if (!directory.isDirectory()) {
      return;
    }

    File[] files = directory.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          if (MODULES_TO_SKIP.contains(file.getName())) {
            System.out.println("Skipping Module: " + file.getAbsolutePath());
            continue;
          }
          findPomFiles(file, entries);
        } else if (file.getName().equals("pom.xml")) {
          try {
            Document doc = Jsoup.parse(Files.readString(file.toPath()), "", Parser.xmlParser());
            addEntry(doc, entries);

          } catch (Exception e) {
            System.out.println("Skipping POM: " + file.getAbsolutePath());
          }
        }
      }
    }
  }

  private static String getTagValue(String current, Element child, String tag) {
    if (child.tag().toString().equals(tag)) {
      return child.toString().replace("<" + tag + ">", "").replace("</" + tag + ">", "");
    }
    return current;
  }

  private static Elements getChildren(Document doc, String tag) {
    return doc.select(tag).get(0).children();
  }

  private static void addEntry(Document doc, List<List<String>> entries) {
    String groupId = null;
    String artifactId = null;

    boolean isPom = false;
    try {
      isPom =
          getChildren(doc, "project")
              .select("packaging")
              .toString()
              .equals("<packaging>pom</packaging>");
    } catch (Exception ignored) {
    }
    if (isPom) {
      throw new IllegalStateException("Cannot add pom packaged module.");
    }

    List<Element> children = getChildren(doc, "project");
    for (Element child : children) {
      groupId = getTagValue(groupId, child, GROUP_ID);
      artifactId = getTagValue(artifactId, child, ARTIFACT_ID);
    }
    if (Strings.isNullOrEmpty(groupId)) {
      groupId = getTagValue(groupId, getChildren(doc, "parent").select(GROUP_ID).get(0), GROUP_ID);
    }
    if (Strings.isNullOrEmpty(groupId) || Strings.isNullOrEmpty(artifactId)) {
      throw new IllegalStateException("Missing " + GROUP_ID + " or " + ARTIFACT_ID + ".");
    }

    entries.add(List.of(groupId, artifactId));
  }
}
