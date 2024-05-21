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

import com.google.ads.googleads.v14.services.GoogleAdsRow;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.options.BigQueryCommonOptions.WriteOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.transforms.GoogleAdsRowToReportRowJsonFn;
import com.google.cloud.teleport.v2.utils.GCSUtils;
import com.google.cloud.teleport.v2.utils.GoogleAdsRateLimitPolicyFactory;
import com.google.cloud.teleport.v2.utils.GoogleAdsUtils;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.googleads.GoogleAdsIO;
import org.apache.beam.sdk.io.googleads.GoogleAdsOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template for writing <a href=
 * "https://developers.google.com/google-ads/api/docs/reporting/overview">Google Ads reports</a> to
 * BigQuery.
 *
 * <p>Nested fields are lifted to top-level fields by replacing the dots in field paths with
 * underscores.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/README_Google_Ads_to_BigQuery.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Google_Ads_to_BigQuery",
    category = TemplateCategory.BATCH,
    displayName = "Google Ads to BigQuery",
    description =
        "The Google Ads to BigQuery template is a batch pipeline that reads Google Ads reports and writes to BigQuery.",
    optionsClass = GoogleAdsToBigQuery.GoogleAdsToBigQueryOptions.class,
    flexContainerName = "google-ads-to-bigquery",
    contactInformation = "https://cloud.google.com/support",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/google-ads-to-bigquery",
    requirements = {
      "The Google Ads account IDs to use.",
      "The Google Ads Query Language query to obtain information.",
      "OAuth credentials for the Google Ads API."
    },
    preview = true)
public final class GoogleAdsToBigQuery {
  public interface GoogleAdsToBigQueryOptions extends WriteOptions, GoogleAdsOptions {
    @TemplateParameter.Long(
        order = 1,
        optional = true,
        description = "Google Ads manager account ID",
        helpText = "A Google Ads manager account ID to use to access the account IDs.",
        example = "12345")
    Long getLoginCustomerId();

    void setLoginCustomerId(Long loginCustomerId);

    @TemplateParameter.Text(
        order = 2,
        regexes = {"^[0-9]+(,[0-9]+)*$"},
        description = "Google Ads account IDs",
        helpText = "A list of Google Ads account IDs to use to execute the query.",
        example = "12345,67890")
    @Validation.Required
    List<Long> getCustomerIds();

    void setCustomerIds(List<Long> customerIds);

    @TemplateParameter.Text(
        order = 3,
        description = "Google Ads Query Language query",
        helpText =
            "The query to use to get the data. See Google Ads Query Language. For example: `SELECT campaign.id, campaign.name FROM campaign`.",
        example = "SELECT campaign.id, campaign.name FROM campaign")
    @Validation.Required
    String getQuery();

    void setQuery(String query);

    @TemplateParameter.Double(
        order = 4,
        description = "Required Google Ads request rate per worker",
        helpText =
            "The rate of query requests per second (QPS) to submit to Google Ads.  "
                + "Divide the desired per pipeline QPS by the maximum number of workers. "
                + "Avoid exceeding per-account or developer token limits. "
                + "See Rate Limits (https://developers.google.com/google-ads/api/docs/best-practices/rate-limits).")
    Double getQpsPerWorker();

    void setQpsPerWorker(Double qpsPerWorker);

    @TemplateParameter.GcsReadFile(
        order = 5,
        optional = true,
        description = "BigQuery Table Schema Path",
        helpText =
            "The Cloud Storage path to the BigQuery schema JSON file. "
                + "If this value is not set, then the schema is inferred "
                + "from the Proto schema.",
        example = "gs://MyBucket/bq_schema.json")
    String getBigQueryTableSchemaPath();

    void setBigQueryTableSchemaPath(String value);

    @TemplateParameter.Text(
        order = 6,
        description = "OAuth 2.0 Client ID identifying the application",
        helpText =
            "The OAuth 2.0 client ID that identifies the application. See Create a client ID and client secret (https://developers.google.com/google-ads/api/docs/oauth/cloud-project#create_a_client_id_and_client_secret).")
    String getGoogleAdsClientId();

    void setGoogleAdsClientId(String clientId);

    @TemplateParameter.Password(
        order = 7,
        description = "OAuth 2.0 Client Secret for the specified Client ID",
        helpText =
            "The OAuth 2.0 client secret that corresponds to the specified client ID. See Create a client ID and client secret (https://developers.google.com/google-ads/api/docs/oauth/cloud-project#create_a_client_id_and_client_secret).")
    String getGoogleAdsClientSecret();

    void setGoogleAdsClientSecret(String clientSecret);

    @TemplateParameter.Password(
        order = 8,
        description = "OAuth 2.0 Refresh Token for the user connecting to the Google Ads API",
        helpText =
            "The OAuth 2.0 refresh token to use to connect to the Google Ads API. See 2-Step Verification (https://developers.google.com/google-ads/api/docs/oauth/2sv).")
    String getGoogleAdsRefreshToken();

    void setGoogleAdsRefreshToken(String refreshToken);

    @TemplateParameter.Password(
        order = 9,
        description = "Google Ads developer token for the user connecting to the Google Ads API",
        helpText =
            "The Google Ads developer token to use to connect to the Google Ads API. See Obtain a developer token (https://developers.google.com/google-ads/api/docs/get-started/dev-token).")
    String getGoogleAdsDeveloperToken();

    void setGoogleAdsDeveloperToken(String developerToken);
  }

  private static final Logger LOG = LoggerFactory.getLogger(GoogleAdsToBigQuery.class);

  public static void main(String[] args) {
    run(
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(GoogleAdsToBigQueryOptions.class));
  }

  public static PipelineResult run(GoogleAdsToBigQueryOptions options) {
    Pipeline pipeline = Pipeline.create(options);
    double qps = options.getQpsPerWorker();
    String query = options.getQuery();

    PCollection<GoogleAdsRow> googleAdsRows =
        pipeline
            .apply(
                Create.of(
                    options.getCustomerIds().stream()
                        .map(Object::toString)
                        .collect(ImmutableList.toImmutableList())))
            .apply(
                GoogleAdsIO.v14()
                    .read()
                    .withDeveloperToken(options.getGoogleAdsDeveloperToken())
                    .withLoginCustomerId(options.getLoginCustomerId())
                    .withQuery(options.getQuery())
                    .withRateLimitPolicy(new GoogleAdsRateLimitPolicyFactory(qps)));

    PCollection<String> reportRows =
        googleAdsRows.apply(ParDo.of(new GoogleAdsRowToReportRowJsonFn(query)));

    Write<String> write =
        BigQueryIO.<String>write()
            .withoutValidation()
            .withWriteDisposition(WriteDisposition.valueOf(options.getWriteDisposition()))
            .withCreateDisposition(CreateDisposition.valueOf(options.getCreateDisposition()))
            .withFormatFunction(BigQueryConverters::convertJsonToTableRow)
            .to(options.getOutputTableSpec());

    String schemaPath = options.getBigQueryTableSchemaPath();

    if (Strings.isNullOrEmpty(schemaPath)) {
      write = write.withSchema(GoogleAdsUtils.createBigQuerySchema(query));
    } else {
      write = write.withJsonSchema(GCSUtils.getGcsFileAsString(schemaPath));
    }

    reportRows.apply("WriteToBigQuery", write);

    return pipeline.run();
  }
}
