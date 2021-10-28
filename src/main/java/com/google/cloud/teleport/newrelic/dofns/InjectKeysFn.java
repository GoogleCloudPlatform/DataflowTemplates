package com.google.cloud.teleport.newrelic.dofns;

import com.google.cloud.teleport.newrelic.dtos.NewRelicLogRecord;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

import static com.google.cloud.teleport.newrelic.utils.ConfigHelper.valueOrDefault;

/**
 * The InjectKeysFn associates a numeric Key, between 0 (inclusive) and "specifiedParallelism" (exclusive),
 * to each of the {@link NewRelicLogRecord}s it processes. This will effectively distribute the processing of
 * such log records (in a multi-worker cluster), since all the log records having the same key will be processed
 * by the same worker.
 */
public class InjectKeysFn extends DoFn<NewRelicLogRecord, KV<Integer, NewRelicLogRecord>> {

    private static final Logger LOG = LoggerFactory.getLogger(InjectKeysFn.class);
    private static final Integer DEFAULT_PARALLELISM = 1;

    private final ValueProvider<Integer> specifiedParallelism;
    private Integer calculatedParallelism;

    public InjectKeysFn(ValueProvider<Integer> specifiedParallelism) {
        this.specifiedParallelism = specifiedParallelism;
    }

    @Setup
    public void setup() {
        calculatedParallelism = valueOrDefault(specifiedParallelism, DEFAULT_PARALLELISM);
        LOG.debug("Parallelism set to: {}", calculatedParallelism);
    }

    @ProcessElement
    public void processElement(
            @Element NewRelicLogRecord inputElement,
            OutputReceiver<KV<Integer, NewRelicLogRecord>> outputReceiver) {
        outputReceiver.output(KV.of(ThreadLocalRandom.current().nextInt(calculatedParallelism), inputElement));
    }
}
