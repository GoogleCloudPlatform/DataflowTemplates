package com.google.cloud.teleport.sentinel;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

  /**
   * Class {@link Write} provides a {@link PTransform} that allows writing {@link SentinelEvent}
   * records into a Sentinel HTTP end-point using HTTP POST requests. In the event of
   * an error, a {@link PCollection} of {@link SentinelWriteError} records are returned for further
   * processing or storing into a deadletter sink.
   */
@Experimental(Kind.SOURCE_SINK)
public class SentinelIO {

    private static final Logger LOG = LoggerFactory.getLogger(SentinelIO.class);

    private SentinelIO() {}

    public static Write.Builder writeBuilder() {
      return new AutoValue_SentinelIO_Write.Builder();
    }    

  /**
   * Class {@link Write} provides a {@link PTransform} that allows writing {@link SentinelEvent}
   * records into a Sentinel HTTP Event Collector end-point using HTTP POST requests. In the event of
   * an error, a {@link PCollection} of {@link SentinelWriteError} records are returned for further
   * processing or storing into a deadletter sink.
   */
  @AutoValue
  public abstract static class Write
      extends PTransform<PCollection<SentinelEvent>, PCollection<SentinelWriteError>> {

    @Nullable
    abstract ValueProvider<String> url();

    @Nullable
    abstract ValueProvider<String> token();

    @Nullable
    abstract ValueProvider<String> customerId();

    @Nullable
    abstract ValueProvider<String> logTableName();

    @Nullable
    abstract ValueProvider<Integer> batchCount();

    @Nullable
    abstract ValueProvider<Integer> parallelism();

    @Nullable
    abstract ValueProvider<Boolean> disableCertificateValidation();

    @Override
    public PCollection<SentinelWriteError> expand(PCollection<SentinelEvent> input) {

      LOG.info("Configuring SentinelEventWriter.");
      SentinelEventWriter.Builder builder =
          SentinelEventWriter.newBuilder()
              .withUrl(url())
              .withInputBatchCount(batchCount())
              .withDisableCertificateValidation(disableCertificateValidation())
              .withCustomerId(customerId())
              .withLogTableName(logTableName())
              .withToken((token()));

      SentinelEventWriter writer = builder.build();
      LOG.info("SentinelEventWriter configured");

      // Return a PCollection<SentinelWriteError>
      return input
          .apply("Create KV pairs", CreateKeys.of(parallelism()))
          .apply("Write Sentinel events", ParDo.of(writer))
          .setCoder(SentinelWriteErrorCoder.of());
    }

    /** A builder for creating {@link Write} objects. */
    @AutoValue.Builder
    public abstract static class Builder {

      abstract Builder setUrl(ValueProvider<String> url);

      abstract ValueProvider<String> url();

      abstract Builder setToken(ValueProvider<String> token);

      abstract ValueProvider<String> token();

      abstract ValueProvider<String> customerId();

      abstract Builder setCustomerId(ValueProvider<String> customerId);

      abstract ValueProvider<String> logTableName();

      abstract Builder setLogTableName(ValueProvider<String> logTableName);      

      abstract Builder setBatchCount(ValueProvider<Integer> batchCount);

      abstract Builder setParallelism(ValueProvider<Integer> parallelism);

      abstract Builder setDisableCertificateValidation(
          ValueProvider<Boolean> disableCertificateValidation);

      abstract Write autoBuild();

      /**
       * Method to set the url for event collector.
       *
       * @param url for event collector
       * @return {@link Builder}
       */
      public Builder withUrl(ValueProvider<String> url) {
        checkArgument(url != null, "withURL(url) called with null input.");
        return setUrl(url);
      }

      /**
       * Same as {@link Builder#withUrl(ValueProvider)} but without {@link ValueProvider}.
       *
       * @param url for event collector
       * @return {@link Builder}
       */
      public Builder withUrl(String url) {
        checkArgument(url != null, "withURL(url) called with null input.");
        return setUrl(ValueProvider.StaticValueProvider.of(url));
      }

      /**
       * Method to set the authentication token.
       *
       * @param token Authentication token for event collector
       * @return {@link Builder}
       */
      public Builder withToken(ValueProvider<String> token) {
        checkArgument(token != null, "withToken(token) called with null input.");
        return setToken(token);
      }

      /**
       * Same as {@link Builder#withToken(ValueProvider)} but without {@link ValueProvider}.
       *
       * @param token Authentication token for event collector
       * @return {@link Builder}
       */
      public Builder withToken(String token) {
        checkArgument(token != null, "withToken(token) called with null input.");
        return setToken(ValueProvider.StaticValueProvider.of(token));
      }

      /**
       * Method to set the authentication customerId.
       *
       * @param customerId
       * @return {@link Builder}
       */
      public Builder withCustomerId(ValueProvider<String> customerId) {
        checkArgument(customerId != null, "withCustomerId(customerId) called with null input.");
        return setCustomerId(customerId);
      }

      /**
       * Same as {@link Builder#withCustomerId(ValueProvider)} but without {@link ValueProvider}.
       *
       * @param customerId
       * @return {@link Builder}
       */
      public Builder withCustomerId(String customerId) {
        checkArgument(customerId != null, "withCustomerId(customerId) called with null input.");
        return setCustomerId(ValueProvider.StaticValueProvider.of(customerId));
      }

      /**
       * Method to set logTableName.
       *
       * @param customerId
       * @return {@link Builder}
       */
      public Builder withLogTableName(ValueProvider<String> logTableName) {
        checkArgument(logTableName != null, "withLogTableName(logTableName) called with null input.");
        return setLogTableName(logTableName);
      }      

      /**
       * Same as {@link Builder#withLogTableName(ValueProvider)} but without {@link ValueProvider}.
       *
       * @param customerId
       * @return {@link Builder}
       */
      public Builder withLogTableName(String logTableName) {
        checkArgument(logTableName != null, "withLogTableName(logTableName) called with null input.");
        return setLogTableName(ValueProvider.StaticValueProvider.of(logTableName));
      }    

      /**
       * Method to set the Batch Count.
       *
       * @param batchCount for batching post requests.
       * @return {@link Builder}
       */
      public Builder withBatchCount(ValueProvider<Integer> batchCount) {
        checkArgument(batchCount != null, "withBatchCount(batchCount) called with null input.");
        return setBatchCount(batchCount);
      }

      /**
       * Same as {@link Builder#withBatchCount(ValueProvider)} but without {@link ValueProvider}.
       *
       * @param batchCount for batching post requests.
       * @return {@link Builder}
       */
      public Builder withBatchCount(Integer batchCount) {
        checkArgument(
            batchCount != null,
            "withMaxEventsBatchCount(maxEventsBatchCount) called with null input.");
        return setBatchCount(ValueProvider.StaticValueProvider.of(batchCount));
      }

      /**
       * Method to set the parallelism.
       *
       * @param parallelism for controlling the number of http client connections.
       * @return {@link Builder}
       */
      public Builder withParallelism(ValueProvider<Integer> parallelism) {
        checkArgument(parallelism != null, "withParallelism(parallelism) called with null input.");
        return setParallelism(parallelism);
      }

      /**
       * Same as {@link Builder#withParallelism(ValueProvider)} but without {@link ValueProvider}.
       *
       * @param parallelism controlling the number of http client connections.
       * @return {@link Builder}
       */
      public Builder withParallelism(Integer parallelism) {
        checkArgument(parallelism != null, "withParallelism(parallelism) called with null input.");
        return setParallelism(ValueProvider.StaticValueProvider.of(parallelism));
      }

      /**
       * Method to disable certificate validation.
       *
       * @param disableCertificateValidation for disabling certificate validation.
       * @return {@link Builder}
       */
      public Builder withDisableCertificateValidation(
          ValueProvider<Boolean> disableCertificateValidation) {
        checkArgument(
            disableCertificateValidation != null,
            "withDisableCertificateValidation(disableCertificateValidation) called with null input.");
        return setDisableCertificateValidation(disableCertificateValidation);
      }

      /**
       * Same as {@link Builder#withDisableCertificateValidation(ValueProvider)} but without a
       * {@link ValueProvider}.
       *
       * @param disableCertificateValidation for disabling certificate validation.
       * @return {@link Builder}
       */
      public Builder withDisableCertificateValidation(Boolean disableCertificateValidation) {
        checkArgument(
            disableCertificateValidation != null,
            "withDisableCertificateValidation(disableCertificateValidation) called with null input.");
        return setDisableCertificateValidation(
            ValueProvider.StaticValueProvider.of((disableCertificateValidation)));
      }

      public Write build() {
        checkNotNull(url(), "HEC url is required.");
        checkNotNull(token(), "Authorization token is required.");
        checkNotNull(customerId(), "Customer Id is required.");
        checkNotNull(logTableName(), "logTableName is required.");

        return autoBuild();
      }
    }

    private static class CreateKeys
        extends PTransform<PCollection<SentinelEvent>, PCollection<KV<Integer, SentinelEvent>>> {

      private static final Integer DEFAULT_PARALLELISM = 1;

      private ValueProvider<Integer> requestedKeys;

      private CreateKeys(ValueProvider<Integer> requestedKeys) {
        this.requestedKeys = requestedKeys;
      }

      static CreateKeys of(ValueProvider<Integer> requestedKeys) {
        return new CreateKeys(requestedKeys);
      }

      @Override
      public PCollection<KV<Integer, SentinelEvent>> expand(PCollection<SentinelEvent> input) {

        return input
            .apply("Inject Keys", ParDo.of(new CreateKeysFn(this.requestedKeys)))
            .setCoder(KvCoder.of(BigEndianIntegerCoder.of(), SentinelEventCoder.of()));
      }

      private class CreateKeysFn extends DoFn<SentinelEvent, KV<Integer, SentinelEvent>> {

        private ValueProvider<Integer> specifiedParallelism;
        private Integer calculatedParallelism;

        CreateKeysFn(ValueProvider<Integer> specifiedParallelism) {
          this.specifiedParallelism = specifiedParallelism;
        }

        @Setup
        public void setup() {

          if (calculatedParallelism == null) {

            if (specifiedParallelism != null) {
              calculatedParallelism = specifiedParallelism.get();
            }

            calculatedParallelism =
                MoreObjects.firstNonNull(calculatedParallelism, DEFAULT_PARALLELISM);

            LOG.info("Parallelism set to: {}", calculatedParallelism);
          }
        }

        @ProcessElement
        public void processElement(ProcessContext context) {
          context.output(
              KV.of(ThreadLocalRandom.current().nextInt(calculatedParallelism), context.element()));
        }
      }
    }
  }

}