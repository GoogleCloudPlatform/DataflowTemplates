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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.options.PostgreSQLToBigQueryOptions;

/**
 * A template that copies data from a PostgreSQL database using JDBC to an existing BigQuery table.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/postgresql-to-googlecloud/README_PostgreSQL_to_BigQuery.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "PostgreSQL_to_BigQuery",
    category = TemplateCategory.BATCH,
    displayName = "PostgreSQL to BigQuery",
    description =
        "A pipeline that reads from PostgreSQL and writes to a BigQuery table. JDBC connection"
            + " string, user name and password can be passed in directly as plaintext or encrypted"
            + " using the Google Cloud KMS API.  If the parameter KMSEncryptionKey is specified,"
            + " connectionURL, username, and password should be all in encrypted format.",
    optionsClass = PostgreSQLToBigQueryOptions.class,
    flexContainerName = "postgresql-to-bigquery",
    skipOptions = {"driverJars", "driverClassName"},
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/postgresql-to-bigquery",
    contactInformation = "https://cloud.google.com/support")
public class PostgreSQLToBigQuery extends JdbcToBigQuery {}
