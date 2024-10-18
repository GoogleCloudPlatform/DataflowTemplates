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
package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.metadata.TemplateIgnoreParameter;
import com.google.cloud.teleport.metadata.TemplateParameter;

/** Interface used by the SQLServerToBigQuery pipeline to accept user input. */
public interface SQLServerToBigQueryOptions extends JdbcToBigQueryOptions {

  @TemplateParameter.Text(
      optional = false,
      regexes = {
        "(^jdbc:[a-zA-Z0-9/:@.?_+!*=&-;]+$)|(^([A-Za-z0-9+/]{4}){1,}([A-Za-z0-9+/]{0,3})={0,3}|projects/.*/secrets/.*/versions/.*)"
      },
      groupName = "Source",
      description = "JDBC connection URL string.",
      helpText =
          "The JDBC connection URL string. Can be passed in as a string that's Base64-encoded and then encrypted with a Cloud KMS key, or can be a Secret Manager secret in the form projects/{project}/secrets/{secret}/versions/{secret_version}.",
      example = "jdbc:sqlserver://localhost;databaseName=sampledb")
  String getConnectionURL();

  @TemplateIgnoreParameter
  default String getDriverClassName() {
    return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  }
}
