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
package com.google.cloud.teleport.dfmetrics.model;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Class {@link CommandLineArgs} represents various input options that can be supplied by the user.
 */
public class CommandLineArgs {
  @Parameter private List<String> parameters = new ArrayList<>();

  public class FileConverter implements IStringConverter<File> {
    @Override
    public File convert(String value) {
      return new File(value);
    }
  }

  @Parameter(
      names = {"--command", "-c"},
      description = "Command name",
      required = true)
  public String command;

  @Parameter(
      names = {"--conf", "-cf"},
      description = "Configuration file containing the pipeline details",
      required = true,
      converter = FileConverter.class)
  public File configFile;

  @Parameter(
      names = {"--output_type", "-ot"},
      description = "Output location type (e.g: bigquery, file)",
      required = true)
  public String outputType;

  @Parameter(
      names = {"--output_location", "-ol"},
      description =
          "Path to output location. For bigquery: project.dataset.table, file:/path/to/file",
      required = true)
  public String outputLocation;
}
