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
import com.google.cloud.teleport.v2.options.OracleToBigQueryOptions;

/**
 * A template that copies data from an Oracle database using JDBC to an existing BigQuery table.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/oracle-to-googlecloud/README_Oracle_to_BigQuery.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Oracle_to_BigQuery",
    category = TemplateCategory.BATCH,
    displayName = "Oracle to BigQuery",
    description =
        "The Oracle to BigQuery template is a batch pipeline that copies data from an Oracle table into an existing BigQuery table. "
            + "This pipeline uses JDBC to connect to Oracle. "
            + "For an extra layer of protection, you can also pass in a Cloud KMS key along with Base64-encoded username, password, and connection string parameters encrypted with the Cloud KMS key. "
            + "For more information about encrypting your username, password, and connection string parameters, see the <a href=\"https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt\">Cloud KMS API encryption endpoint</a>.",
    optionsClass = OracleToBigQueryOptions.class,
    flexContainerName = "oracle-to-bigquery",
    skipOptions = {"driverJars", "driverClassName"},
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/oracle-to-bigquery",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "If BigQuery table already exist before pipeline execution, it must have a compatible schema.",
      "The Oracle database must be accessible from the subnetwork where Dataflow runs."
    })
public class OracleToBigQuery extends JdbcToBigQuery {}
