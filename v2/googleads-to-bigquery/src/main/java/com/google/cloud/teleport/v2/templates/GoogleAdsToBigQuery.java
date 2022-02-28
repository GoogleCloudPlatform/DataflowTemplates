/*
 * Copyright (C) 2022 Google LLC
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

import com.google.ads.googleads.v10.services.GoogleAdsRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.v2.io.GoogleAdsIO;
import com.google.cloud.teleport.v2.io.RateLimitPolicyFactory;
import com.google.cloud.teleport.v2.options.BigQueryCommonOptions.WriteOptions;
import com.google.cloud.teleport.v2.options.GoogleAdsOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.transforms.GoogleAdsRowToReportRowJsonFn;
import com.google.cloud.teleport.v2.utils.GoogleAdsUtils;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * A template for writing <a
 * href="https://developers.google.com/google-ads/api/docs/reporting/overview">Google Ads
 * reports</a> to BigQuery.
 *
 * <p>Nested fields are lifted to top-level fields by replacing the dots in field paths with
 * underscores.
 */
public final class GoogleAdsToBigQuery {

  /** {@link org.apache.beam.sdk.options.PipelineOptions} for {@link GoogleAdsToBigQuery} */
  public interface GoogleAdsToBigQueryOptions extends GoogleAdsOptions, WriteOptions {
    @Description("Developer token for the Google Ads API.")
    String getGoogleAdsDeveloperToken();

    void setGoogleAdsDeveloperToken(String googleAdsDeveloperToken);

    @Description("Manager account ID to use during Google Ads API calls.")
    Long getLoginCustomerId();

    void setLoginCustomerId(Long loginCustomerId);

    @Description("List of customer IDs to run the Google Ads Query Langugage Query query for.")
    @Validation.Required
    List<Long> getCustomerIds();

    void setCustomerIds(List<Long> customerIds);

    @Description("A Google Ads Query Language query.")
    @Validation.Required
    String getQuery();

    void setQuery(String query);

    @Description("Maximum number of API call retries before giving up.")
    @Default.Integer(5)
    Integer getMaxRetries();

    void setMaxRetries(Integer maxRetries);

    @Description("Maximum number of queries per second.")
    @Default.Double(5.0)
    Double getMaxQueriesPerSecond();

    void setMaxQueriesPerSecond(Double maxQueriesPerSecond);
  }

  public static void main(String[] args) {
    run(
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(GoogleAdsToBigQueryOptions.class));
  }

  public static PipelineResult run(GoogleAdsToBigQueryOptions options) {
    Pipeline pipeline = Pipeline.create(options);
    PCollection<GoogleAdsRow> googleAdsRows =
        pipeline.apply(
            GoogleAdsIO.read()
                .withDeveloperToken(options.getGoogleAdsDeveloperToken())
                .withLoginCustomerId(options.getLoginCustomerId())
                .withCustomerIds(options.getCustomerIds())
                .withQuery(options.getQuery())
                .withRateLimitPolicy(
                    RateLimitPolicyFactory.withDefaultRateLimiter(
                        options.getMaxRetries(), options.getMaxQueriesPerSecond())));

    TableSchema schema = GoogleAdsUtils.createBigQuerySchema(options.getQuery());
    PCollection<String> reportRows =
        googleAdsRows.apply(ParDo.of(new GoogleAdsRowToReportRowJsonFn(options.getQuery())));

    reportRows.apply(
        BigQueryConverters.<String>createWriteTransform(options)
            .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
            .withFormatFunction(BigQueryConverters::convertJsonToTableRow)
            .withSchema(schema));

    return pipeline.run();
  }
}
