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
package com.google.cloud.teleport.v2.io;

import com.google.ads.googleads.v10.errors.GoogleAdsException;
import com.google.ads.googleads.v10.errors.GoogleAdsFailure;
import com.google.ads.googleads.v10.errors.QuotaErrorDetails;
import com.google.ads.googleads.v10.errors.QuotaErrorEnum;
import com.google.ads.googleads.v10.services.GoogleAdsRow;
import com.google.ads.googleads.v10.services.GoogleAdsServiceClient;
import com.google.ads.googleads.v10.services.SearchGoogleAdsStreamRequest;
import com.google.ads.googleads.v10.services.SearchGoogleAdsStreamResponse;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.options.GoogleAdsOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoMessageSchema;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.nullness.qual.Nullable;

public class GoogleAdsIO {
  public static Read read() {
    return new AutoValue_GoogleAdsIO_Read.Builder()
        .setGoogleAdsClientFactory(DefaultGoogleAdsClientFactory.getInstance())
        .setRateLimitPolicyFactory(RateLimitPolicyFactory.withoutRatelimiter())
        .setInferBeamSchema(false)
        .build();
  }

  public static ReadAll readAll() {
    return new AutoValue_GoogleAdsIO_ReadAll.Builder()
        .setGoogleAdsClientFactory(DefaultGoogleAdsClientFactory.getInstance())
        .setRateLimitPolicyFactory(RateLimitPolicyFactory.withoutRatelimiter())
        .setInferBeamSchema(false)
        .build();
  }

  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<GoogleAdsRow>> {
    abstract @Nullable ValueProvider<String> getDeveloperToken();

    abstract @Nullable ValueProvider<Long> getLoginCustomerId();

    abstract @Nullable ValueProvider<List<Long>> getCustomerIds();

    abstract @Nullable ValueProvider<String> getQuery();

    abstract GoogleAdsClientFactory getGoogleAdsClientFactory();

    abstract RateLimitPolicyFactory getRateLimitPolicyFactory();

    abstract boolean getInferBeamSchema();

    abstract Builder toBuilder();

    public Read withDeveloperToken(ValueProvider<String> developerToken) {
      return toBuilder().setDeveloperToken(developerToken).build();
    }

    public Read withDeveloperToken(String developerToken) {
      return withDeveloperToken(StaticValueProvider.of(developerToken));
    }

    public Read withLoginCustomerId(ValueProvider<Long> loginCustomerId) {
      return toBuilder().setLoginCustomerId(loginCustomerId).build();
    }

    public Read withLoginCustomerId(Long loginCustomerId) {
      return withLoginCustomerId(StaticValueProvider.of(loginCustomerId));
    }

    public Read withCustomerIds(ValueProvider<List<Long>> customerIds) {
      return toBuilder().setCustomerIds(customerIds).build();
    }

    public Read withCustomerIds(List<Long> customerIds) {
      return withCustomerIds(StaticValueProvider.of(customerIds));
    }

    public Read withCustomerId(ValueProvider<Long> customerId) {
      return withCustomerIds(NestedValueProvider.of(customerId, ImmutableList::of));
    }

    public Read withCustomerId(Long customerId) {
      return withCustomerIds(StaticValueProvider.of(ImmutableList.of(customerId)));
    }

    public Read withQuery(ValueProvider<String> query) {
      return toBuilder().setQuery(query).build();
    }

    public Read withQuery(String query) {
      return withQuery(StaticValueProvider.of(query));
    }

    public Read withGoogleAdsClientFactory(GoogleAdsClientFactory googleAdsClientFactory) {
      return toBuilder().setGoogleAdsClientFactory(googleAdsClientFactory).build();
    }

    public Read withRateLimitPolicy(RateLimitPolicyFactory rateLimitPolicyFactory) {
      return toBuilder().setRateLimitPolicyFactory(rateLimitPolicyFactory).build();
    }

    public Read withBeamSchemas(boolean withBeamSchemas) {
      return toBuilder().setInferBeamSchema(withBeamSchemas).build();
    }

    @Override
    public PCollection<GoogleAdsRow> expand(PBegin input) {
      return input
          .apply(Create.ofProvider(getCustomerIds(), ListCoder.of(BigEndianLongCoder.of())))
          .apply(FlatMapElements.into(TypeDescriptors.longs()).via(customerIds -> customerIds))
          .apply(
              MapElements.into(TypeDescriptor.of(SearchGoogleAdsStreamRequest.class))
                  .via(
                      customerId ->
                          SearchGoogleAdsStreamRequest.newBuilder()
                              .setCustomerId(Long.toString(customerId))
                              .setQuery(getQuery().get())
                              .build()))
          .apply(
              readAll()
                  .withDeveloperToken(getDeveloperToken())
                  .withLoginCustomerId(getLoginCustomerId())
                  .withGoogleAdsClientFactory(getGoogleAdsClientFactory())
                  .withBeamSchemas(getInferBeamSchema()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(DisplayData.item("customerIds", getCustomerIds()).withLabel("Customer IDs"))
          .add(DisplayData.item("query", getQuery()).withLabel("Query"));
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDeveloperToken(ValueProvider<String> developerToken);

      abstract Builder setLoginCustomerId(ValueProvider<Long> loginCustomerId);

      abstract Builder setCustomerIds(ValueProvider<List<Long>> customerId);

      abstract Builder setQuery(ValueProvider<String> query);

      abstract Builder setGoogleAdsClientFactory(GoogleAdsClientFactory googleAdsClientFactory);

      abstract Builder setRateLimitPolicyFactory(RateLimitPolicyFactory rateLimitPolicyFactory);

      abstract Builder setInferBeamSchema(boolean inferBeamSchema);

      abstract Read build();
    }
  }

  @AutoValue
  public abstract static class ReadAll
      extends PTransform<PCollection<SearchGoogleAdsStreamRequest>, PCollection<GoogleAdsRow>> {
    private static SchemaCoder<GoogleAdsRow> createSchemaCoder() {
      ProtoMessageSchema protoMessageSchema = new ProtoMessageSchema();
      TypeDescriptor<GoogleAdsRow> typeDescriptor = TypeDescriptor.of(GoogleAdsRow.class);
      return SchemaCoder.of(
          protoMessageSchema.schemaFor(typeDescriptor),
          typeDescriptor,
          protoMessageSchema.toRowFunction(typeDescriptor),
          protoMessageSchema.fromRowFunction(typeDescriptor));
    }

    abstract @Nullable ValueProvider<String> getDeveloperToken();

    abstract @Nullable ValueProvider<Long> getLoginCustomerId();

    abstract GoogleAdsClientFactory getGoogleAdsClientFactory();

    abstract RateLimitPolicyFactory getRateLimitPolicyFactory();

    abstract boolean getInferBeamSchema();

    abstract Builder toBuilder();

    public ReadAll withDeveloperToken(ValueProvider<String> developerToken) {

      return toBuilder().setDeveloperToken(developerToken).build();
    }

    public ReadAll withDeveloperToken(String developerToken) {
      return withDeveloperToken(StaticValueProvider.of(developerToken));
    }

    public ReadAll withLoginCustomerId(ValueProvider<Long> loginCustomerId) {
      return toBuilder().setLoginCustomerId(loginCustomerId).build();
    }

    public ReadAll withLoginCustomerId(Long loginCustomerId) {
      return withLoginCustomerId(StaticValueProvider.of(loginCustomerId));
    }

    public ReadAll withGoogleAdsClientFactory(GoogleAdsClientFactory googleAdsClientFactory) {
      return toBuilder().setGoogleAdsClientFactory(googleAdsClientFactory).build();
    }

    public ReadAll withRateLimitPolicy(RateLimitPolicyFactory rateLimitPolicyFactory) {
      return toBuilder().setRateLimitPolicyFactory(rateLimitPolicyFactory).build();
    }

    public ReadAll withBeamSchemas(boolean withBeamSchemas) {
      return toBuilder().setInferBeamSchema(withBeamSchemas).build();
    }

    @Override
    public PCollection<GoogleAdsRow> expand(PCollection<SearchGoogleAdsStreamRequest> input) {
      PCollection<GoogleAdsRow> read = input.apply(ParDo.of(new ReadAllFn(this)));
      ProtoMessageSchema schema = new ProtoMessageSchema();
      return getInferBeamSchema() ? read.setCoder(createSchemaCoder()) : read;
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDeveloperToken(ValueProvider<String> developerToken);

      abstract Builder setLoginCustomerId(ValueProvider<Long> loginCustomerId);

      abstract Builder setGoogleAdsClientFactory(GoogleAdsClientFactory googleAdsClientFactory);

      abstract Builder setRateLimitPolicyFactory(RateLimitPolicyFactory rateLimitPolicyFactory);

      abstract Builder setInferBeamSchema(boolean inferBeamSchema);

      abstract ReadAll build();
    }

    @VisibleForTesting
    static class ReadAllFn extends DoFn<SearchGoogleAdsStreamRequest, GoogleAdsRow> {
      private final Counter accountQuotaErrors =
          Metrics.counter(ReadAllFn.class, "account_quota_errors");
      private final Counter developerQuotaErrors =
          Metrics.counter(ReadAllFn.class, "developer_quota_errors");
      private final GoogleAdsIO.ReadAll spec;
      private transient GoogleAdsServiceClient googleAdsServiceClient;
      private transient RateLimitPolicy rateLimitPolicy;

      ReadAllFn(GoogleAdsIO.ReadAll spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() {
        rateLimitPolicy = spec.getRateLimitPolicyFactory().getRateLimitPolicy();
      }

      @StartBundle
      public void startBundle(StartBundleContext startBundleContext) {
        GoogleAdsOptions googleAdsOptions =
            startBundleContext.getPipelineOptions().as(GoogleAdsOptions.class);
        googleAdsServiceClient =
            spec.getGoogleAdsClientFactory()
                .newGoogleAdsClient(
                    spec.getDeveloperToken().get(),
                    null,
                    spec.getLoginCustomerId().get(),
                    googleAdsOptions)
                .getLatestVersion()
                .createGoogleAdsServiceClient();
      }

      @FinishBundle
      public void finishBundle() {
        googleAdsServiceClient.close();
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws IOException, InterruptedException {
        SearchGoogleAdsStreamRequest request = c.element();
        while (true) {
          rateLimitPolicy.onBeforeRequest(request);
          try {
            for (SearchGoogleAdsStreamResponse response :
                googleAdsServiceClient.searchStreamCallable().call(request)) {
              for (GoogleAdsRow row : response.getResultsList()) {
                c.output(row);
              }
            }
            rateLimitPolicy.onSuccess();
            return;
          } catch (GoogleAdsException e) {
            GoogleAdsFailure failure = e.getGoogleAdsFailure();
            QuotaErrorDetails quotaErrorDetails =
                failure.getErrorsList().stream()
                    .filter(
                        err ->
                            err.getErrorCode().getQuotaError()
                                    == QuotaErrorEnum.QuotaError.RESOURCE_EXHAUSTED
                                || err.getErrorCode().getQuotaError()
                                    == QuotaErrorEnum.QuotaError.RESOURCE_TEMPORARILY_EXHAUSTED)
                    .map(err -> err.getDetails().getQuotaErrorDetails())
                    .findFirst()
                    .orElseThrow(() -> new IOException(e));
            switch (quotaErrorDetails.getRateScope()) {
              case ACCOUNT:
                accountQuotaErrors.inc();
                break;
              case DEVELOPER:
                developerQuotaErrors.inc();
                break;
            }
            rateLimitPolicy.onQuotaError(quotaErrorDetails);
          }
        }
      }
    }
  }
}
