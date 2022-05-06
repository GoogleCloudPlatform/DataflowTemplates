package com.google.cloud.teleport.v2.templates;

import org.apache.beam.sdk.io.splunk.SplunkEvent;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.examples.complete.datatokenization.utils.FailsafeElement;

public class GCSToSplunk {

  /** The tag for successful {@link SplunkEvent} conversion. */
  private static final TupleTag<SplunkEvent> SPLUNK_EVENT_OUT = new TupleTag<SplunkEvent>() {};

  /** The tag for failed {@link SplunkEvent} conversion. */
  private static final TupleTag<FailsafeElement<String, String>> SPLUNK_EVENT_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};
}