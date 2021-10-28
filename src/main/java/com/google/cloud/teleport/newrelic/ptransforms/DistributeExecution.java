package com.google.cloud.teleport.newrelic.ptransforms;

import com.google.cloud.teleport.newrelic.dofns.InjectKeysFn;
import com.google.cloud.teleport.newrelic.dtos.NewRelicLogRecord;
import com.google.cloud.teleport.newrelic.dtos.coders.NewRelicLogRecordCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * This PTransform adds a Key to each processed {@link NewRelicLogRecord}, resulting in a key-value pair (where the
 * value is the {@link NewRelicLogRecord}. This will effectively parallelize the execution, since all the records having
 * the same key will be processed by the same worker instance.
 */
public class DistributeExecution extends
        PTransform<PCollection<NewRelicLogRecord>, PCollection<KV<Integer, NewRelicLogRecord>>> {

    private final ValueProvider<Integer> specifiedParallelism;

    private DistributeExecution(ValueProvider<Integer> specifiedParallelism) {
        this.specifiedParallelism = specifiedParallelism;
    }

    public static DistributeExecution withParallelism(ValueProvider<Integer> specifiedParallelism) {
        return new DistributeExecution(specifiedParallelism);
    }

    @Override
    public PCollection<KV<Integer, NewRelicLogRecord>> expand(PCollection<NewRelicLogRecord> input) {

        return input
                .apply("Inject Keys", ParDo.of(new InjectKeysFn(this.specifiedParallelism)))
                .setCoder(KvCoder.of(BigEndianIntegerCoder.of(), NewRelicLogRecordCoder.getInstance()));
    }
}