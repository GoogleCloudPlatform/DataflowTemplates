package com.google.cloud.teleport.newrelic.ptransforms;

import com.google.cloud.teleport.newrelic.dtos.NewRelicLogRecord;
import com.google.cloud.teleport.newrelic.dofns.NewRelicLogRecordWriterFn;
import com.google.cloud.teleport.newrelic.dtos.NewRelicLogApiSendError;
import com.google.cloud.teleport.newrelic.dtos.coders.NewRelicLogApiSendErrorCoder;
import com.google.cloud.teleport.newrelic.config.NewRelicConfig;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class {@link NewRelicIO} provides a {@link PTransform} that allows writing {@link NewRelicLogRecord}
 * records into a New Relic Logs API endpoint using HTTP POST requests. In the event of
 * an error, a {@link PCollection} of {@link NewRelicLogApiSendError} records are returned for further
 * processing or storing into a deadletter sink.
 */
public class NewRelicIO extends PTransform<PCollection<NewRelicLogRecord>, PCollection<NewRelicLogApiSendError>> {

    private static final Logger LOG = LoggerFactory.getLogger(NewRelicIO.class);

    private final NewRelicConfig newRelicConfig;

    public NewRelicIO(NewRelicConfig newRelicConfig) {
        this.newRelicConfig = newRelicConfig;
    }

    @Override
    public PCollection<NewRelicLogApiSendError> expand(PCollection<NewRelicLogRecord> input) {
        LOG.debug("Configuring NewRelicRecordWriter.");
        NewRelicLogRecordWriterFn writer = new NewRelicLogRecordWriterFn(newRelicConfig);

        return input
                .apply("Distribute execution", DistributeExecution.withParallelism(newRelicConfig.getParallelism()))
                .apply("Send logs to New Relic", ParDo.of(writer)).setCoder(NewRelicLogApiSendErrorCoder.getInstance());
    }
}
