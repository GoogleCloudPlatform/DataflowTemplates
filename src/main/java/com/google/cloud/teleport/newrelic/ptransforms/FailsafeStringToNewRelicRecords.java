package com.google.cloud.teleport.newrelic.ptransforms;

import com.google.api.client.util.DateTime;
import com.google.cloud.teleport.newrelic.dtos.NewRelicLogRecord;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.base.Throwables;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms messages (either as plain strings or as JSON strings) to {@link NewRelicLogRecord}s.
 */
public class FailsafeStringToNewRelicRecords extends PTransform<PCollection<FailsafeElement<String, String>>, PCollectionTuple> {

  private static final Logger LOG = LoggerFactory.getLogger(FailsafeStringToNewRelicRecords.class);
  private static final String TIMESTAMP_KEY = "timestamp";
  private static final Counter CONVERSION_ERRORS = Metrics.counter(FailsafeStringToNewRelicRecords.class, "newrelic-event-conversion-errors");
  private static final Counter CONVERSION_SUCCESS = Metrics.counter(FailsafeStringToNewRelicRecords.class, "newrelic-event-conversion-successes");

  private TupleTag<NewRelicLogRecord> successfulConversionsTag;
  private TupleTag<FailsafeElement<String, String>> failedConversionsTag;

  private FailsafeStringToNewRelicRecords(TupleTag<NewRelicLogRecord> successfulConversionsTag,
                                          TupleTag<FailsafeElement<String, String>> failedConversionsTag) {
    this.successfulConversionsTag = successfulConversionsTag;
    this.failedConversionsTag = failedConversionsTag;
  }

  /**
   * Returns a {@link FailsafeStringToNewRelicRecords} {@link PTransform} that
   * consumes {@link FailsafeElement} messages and creates {@link NewRelicLogRecord}
   * objects. Any conversion errors are wrapped into a {@link FailsafeElement}
   * with appropriate error information.
   *
   * @param successfulConversionsTag {@link TupleTag} to use for successfully converted messages.
   * @param failedConversionsTag     {@link TupleTag} to use for messages that failed conversion.
   */
  public static FailsafeStringToNewRelicRecords withOutputTags(TupleTag<NewRelicLogRecord> successfulConversionsTag,
                                                               TupleTag<FailsafeElement<String, String>> failedConversionsTag) {
    return new FailsafeStringToNewRelicRecords(successfulConversionsTag, failedConversionsTag);
  }

  @Override
  public PCollectionTuple expand(PCollection<FailsafeElement<String, String>> input) {

    return input.apply("Convert to NewRelicLogRecord", ParDo.of(new DoFn<FailsafeElement<String, String>, NewRelicLogRecord>() {

      @ProcessElement
      public void processElement(
        @Element FailsafeElement<String, String> inputElement,
        MultiOutputReceiver outputReceivers
      ) {
        final String input = inputElement.getPayload();

        try {
          final Long timestamp = extractTimestampEpochMillisFromJson(input);

          outputReceivers.get(successfulConversionsTag).output(new NewRelicLogRecord(input, timestamp));
          CONVERSION_SUCCESS.inc();
        } catch (Exception e) {
          CONVERSION_ERRORS.inc();
          outputReceivers.get(failedConversionsTag).output(FailsafeElement.of(input, input).setErrorMessage(e.getMessage())
            .setStacktrace(Throwables.getStackTraceAsString(e)));
        }
      }
    }).withOutputTags(successfulConversionsTag, TupleTagList.of(failedConversionsTag)));
  }

  /**
   * Utilitary method to extract the "timestamp" field from a potentially JSON-formatted string as epoch milliseconds.
   *
   * @param input Potentially JSON-formatted string. If the String is not JSON-formatted, this method returns null
   * @return The epoch milliseconds, if the input is a JSON-formatted string with a "timestamp" field in ISO8601. Otherwise, null.
   */
  private static Long extractTimestampEpochMillisFromJson(final String input) {
    // We attempt to parse the input to see if it is a valid JSON and if so, whether we can extract some
    // additional properties that would be present in Stackdriver's LogEntry structure (timestamp) or
    // a user-provided _metadata field.
    try {
      final JSONObject json = new JSONObject(input);

      String parsedTimestamp = json.optString(TIMESTAMP_KEY);
      if (!parsedTimestamp.isEmpty()) {
        try {
          return DateTime.parseRfc3339(parsedTimestamp).getValue();
        } catch (NumberFormatException n) {
          // We log this exception but don't want to fail the entire record.
          LOG.debug("Unable to parse non-rfc3339 formatted timestamp: {}", parsedTimestamp);
        }
      }
    } catch (JSONException je) {
      // input is either not a properly formatted JSONObject or has other exceptions. In this case, we will
      // simply capture the entire input as a log record and not worry about capturing any specific properties
      // (for e.g Timestamp etc). We also do not want to LOG this as we might be running a pipeline to
      // simply log text entries to NewRelic and this is expected behavior.
    }
    return null;
  }
}
