package com.infusionsoft.dataflow.templates;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadOptions;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubWriteOptions;
import com.google.cloud.teleport.util.DurationUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A template that groups pubsub messages by accountId and then re-emits an aggregate message after
 * a few minutes
 *
 * Used by event-bridge-api
 *
 * Deploy to sand:
 * mvn compile exec:java -Dexec.mainClass=com.infusionsoft.dataflow.templates.AggregatePubsub -Dexec.args="--project=is-event-bridge-api-sand --stagingLocation=gs://dataflow-is-event-bridge-api-sand/staging --templateLocation=gs://dataflow-is-event-bridge-api-sand/templates/ps2aps_5m --runner=DataflowRunner --serviceAccount=is-event-bridge-api-sand@appspot.gserviceaccount.com --windowDuration=5m"
 *
 * projects/is-tracking-pixel-api-sand/topics/v1.render
 * projects/is-event-bridge-api-sand/topics/v1.renders
 *
 * projects/is-tracking-link-api-sand/topics/v1.click
 * projects/is-event-bridge-api-sand/topics/v1.clicks
 *
 * Deploy to prod:
 * mvn compile exec:java -Dexec.mainClass=com.infusionsoft.dataflow.templates.AggregatePubsub -Dexec.args="--project=is-event-bridge-api-prod --stagingLocation=gs://dataflow-is-event-bridge-api-prod/staging --templateLocation=gs://dataflow-is-event-bridge-api-prod/templates/ps2aps_5m --runner=DataflowRunner --serviceAccount=is-event-bridge-api-prod@appspot.gserviceaccount.com --windowDuration=5m"
 *
 * projects/is-tracking-pixel-api-prod/topics/v1.render
 * projects/is-event-bridge-api-prod/topics/v1.renders
 *
 * projects/is-tracking-link-api-prod/topics/v1.click
 * projects/is-event-bridge-api-prod/topics/v1.clicks
 */
public class AggregatePubsub {

  /**
   * Options supported by {@link AggregatePubsub}.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, StreamingOptions,
      PubsubReadOptions, PubsubWriteOptions {

    @Description("The window duration in which data will be written. "
        + "Allowed formats are: "
        + "Ns (for seconds, example: 30s), "
        + "Nm (for minutes, example: 5m), "
        + "Nh (for hours, example: 2h).")
    String getWindowDuration();
    void setWindowDuration(String value);
  }

  public static class GroupByAccountId extends PTransform<PCollection<String>, PCollection<String>> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    public PCollection<String> expand(PCollection<String> input) {
      return input
          .apply(WithKeys.of(new GetAccountIdFn(OBJECT_MAPPER)))
          .apply(GroupByKey.create())
          .apply(new AggregateFn(OBJECT_MAPPER));
    }
  }

  public static class AggregateFn extends PTransform<PCollection<KV<String, Iterable<String>>>, PCollection<String>> {

    private static final Logger LOG = LoggerFactory.getLogger(AggregateFn.class);

    private final ObjectMapper objectMapper;

    public AggregateFn(ObjectMapper objectMapper) {
      this.objectMapper = checkNotNull(objectMapper, "objectMapper must not be null");
    }

    public PCollection<String> expand(PCollection<KV<String, Iterable<String>>> messages) {
      return messages.apply(MapElements.via(new SimpleFunction<KV<String, Iterable<String>>, String>() {
        public String apply(KV<String, Iterable<String>> kv) {
          final Iterable<String> iterable = kv.getValue();
          final Iterator<String> iterator = iterable.iterator();
          final AtomicReference<String> accountId = new AtomicReference<>();

          final Map<String, Object> json = new LinkedHashMap<>();
          json.put("messages", StreamSupport
              .stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(message -> {
                try {
                  final Map<String, Object> map = objectMapper.readValue(message, Map.class);
                  accountId.compareAndSet(null, (String) map.get("accountId"));

                  return map;
                } catch (IOException e) {
                  LOG.error("unable to deserialize: " + message, e);
                  throw new IllegalStateException(e);
                }
              }).collect(Collectors.toList()));
          json.put("accountId", accountId.get());

          try {
            final String aggregated = objectMapper.writeValueAsString(json);
            LOG.debug("Aggregated: {} -> {}", iterable, aggregated);

            return aggregated;
          } catch (JsonProcessingException e) {
            LOG.error("unable to serialize: " + json, e);
            throw new IllegalStateException(e);
          }
        }
      }));
    }
  }

  public static class GetAccountIdFn implements SerializableFunction<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(GetAccountIdFn.class);

    private final ObjectMapper objectMapper;

    public GetAccountIdFn(ObjectMapper objectMapper) {
      this.objectMapper = checkNotNull(objectMapper, "objectMapper must not be null");
    }

    public String apply(String message) {
      try {
        final Map<String, Object> json = objectMapper.readValue(message, Map.class);
        final String accountId = (String) json.get("accountId");

        LOG.debug("{} -> {}", message, accountId);

        return accountId;
      } catch (IOException e) {
        LOG.error("unable to deserialize: " + message, e);
        throw new IllegalStateException(e);
      }
    }
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    // Parse the user options passed from the command-line
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    options.setStreaming(true);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Read Events", PubsubIO.readStrings()
            .fromTopic(options.getPubsubReadTopic()))
        .apply(options.getWindowDuration() + " Window",
            Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))
        .apply("Group By Account", new GroupByAccountId())
        .apply("Write Events", PubsubIO.writeStrings()
            .to(options.getPubsubWriteTopic()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}