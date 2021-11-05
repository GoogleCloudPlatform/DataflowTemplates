package com.google.cloud.teleport.newrelic;

import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.newrelic.dtos.NewRelicLogApiSendError;
import com.google.cloud.teleport.newrelic.dtos.NewRelicLogRecord;
import com.google.cloud.teleport.newrelic.dtos.coders.NewRelicLogRecordCoder;
import com.google.cloud.teleport.newrelic.ptransforms.FailsafeStringToNewRelicRecords;
import com.google.cloud.teleport.values.FailsafeElement;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Builds the pipeline used to forward logs from a PubSub topic to New Relic. Its constructor allows passing
 * both the PTransform used to read the data, and the one to write it. This allows using dependency inversion,
 * which is specially useful when writing tests.
 */
public class NewRelicPipeline {

  /**
   * String/String Coder for FailsafeElement.
   */
  public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
    FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

  /**
   * The tag for successful {@link NewRelicLogRecord} conversion.
   */
  private static final TupleTag<NewRelicLogRecord> SUCCESSFUL_CONVERSIONS = new TupleTag<NewRelicLogRecord>() {
  };

  /**
   * The tag for failed {@link NewRelicLogRecord} conversion.
   */
  private static final TupleTag<FailsafeElement<String, String>> FAILED_CONVERSIONS =
    new TupleTag<FailsafeElement<String, String>>() {
    };

  private final PTransform<PBegin, PCollection<String>> pubsubMessageReaderTransform;
  private final PTransform<PCollection<NewRelicLogRecord>, PCollection<NewRelicLogApiSendError>> newrelicMessageWriterTransform;
  private final Pipeline pipeline;

  public NewRelicPipeline(Pipeline pipeline,
                          PTransform<PBegin, PCollection<String>> pubsubMessageReaderTransform,
                          PTransform<PCollection<NewRelicLogRecord>, PCollection<NewRelicLogApiSendError>> newrelicMessageWriterTransform) {
    this.pipeline = pipeline;
    this.pubsubMessageReaderTransform = pubsubMessageReaderTransform;
    this.newrelicMessageWriterTransform = newrelicMessageWriterTransform;
  }

  /*
   * Runs the pipeline to completion with the specified options. This method does not wait until the
   * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
   * object to block until the pipeline is finished running if blocking programmatic execution is
   * required.
   *
   * @return The pipeline result.
   */
  public PipelineResult run() {

    // Register New relic and failsafe coders.
    CoderRegistry registry = pipeline.getCoderRegistry();
    registry.registerCoderForClass(NewRelicLogRecord.class, NewRelicLogRecordCoder.getInstance());
    registry.registerCoderForType(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    // 1) Read messages in from Pub/Sub
    PCollection<String> stringMessages = pipeline.apply("Read messages from subscription", pubsubMessageReaderTransform);

    // 2) Convert message to FailsafeElement for processing.
    PCollection<FailsafeElement<String, String>> transformedOutput =
      stringMessages.apply(
        "Transform to Failsafe Element",
        MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
          .via(input -> FailsafeElement.of(input, input)));

    // 3) Convert successfully transformed messages into NewRelicRecords objects
    PCollectionTuple convertToEventTuple = transformedOutput
      .apply("Transform to NewRelicLogRecord", FailsafeStringToNewRelicRecords.withOutputTags(SUCCESSFUL_CONVERSIONS, FAILED_CONVERSIONS));

    // 4) Write NewRelicRecords to NewRelic's Log API end point.
    convertToEventTuple
      .get(SUCCESSFUL_CONVERSIONS)
      .apply("Forward logs to New Relic", newrelicMessageWriterTransform);

    return pipeline.run();
  }
}
