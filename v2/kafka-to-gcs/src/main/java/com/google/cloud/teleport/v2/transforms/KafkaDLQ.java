package com.google.cloud.teleport.v2.transforms;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.joda.time.Duration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;


import com.google.auto.value.AutoValue;
import org.springframework.lang.NonNull;


@AutoValue
public abstract class KafkaDLQ extends PTransform<PCollection<BadRecord>, PDone> {

    private static Coder<KafkaRecord<byte[], byte[]>> coder = KafkaRecordCoder.of(
            NullableCoder.of(ByteArrayCoder.of()), NullableCoder.of(ByteArrayCoder.of())
    );
    public abstract String bootStrapServers();
    public abstract String topics();
    public abstract @NonNull Map<String, Object> config();
    public static KafkaDLQBuilder newBuilder() {
        return new AutoValue_KafkaDLQ.Builder();
    }

    @AutoValue.Builder
    public abstract static class KafkaDLQBuilder {
        public abstract KafkaDLQBuilder setBootStrapServers(String value);
        public abstract KafkaDLQBuilder setTopics(String value);

        public abstract KafkaDLQBuilder setConfig(Map<String, Object> value);

        abstract KafkaDLQ autoBuild();
        public KafkaDLQ build() {
            return autoBuild();
        }
    }

    // Overridden expand method
    @Override
    public PDone expand(PCollection<BadRecord> input) {
        return input.apply(ParDo.of(new GetPayloadFromBadRecord()))
                .apply(KafkaIO.<byte[], byte[]>write()
                        .withBootstrapServers(bootStrapServers())
                        .withTopic(topics())
                        .withProducerConfigUpdates(config())
                        .withKeySerializer(ByteArraySerializer.class)
                        .withValueSerializer(ByteArraySerializer.class));
    }

    public static class GetPayloadFromBadRecord extends DoFn<BadRecord, KV<byte[], byte[]>> {
        @ProcessElement
        public void processElement(@Element BadRecord badRecord,
                                   OutputReceiver<KV<byte[], byte[]>> receiver) throws IOException {
            byte[] encodedRecord = badRecord.getRecord().getEncodedRecord();
            InputStream inputStream = new ByteArrayInputStream(encodedRecord);
            // We get the coder from the record, but it is returned as string. Maybe the class name which we
            // can import?
            KafkaRecord<byte[], byte[]> record = coder.decode(inputStream);
            receiver.output(record.getKV());
        }
    }


    public static class ErrorSinkTransform
            extends PTransform<PCollection<BadRecord>, PCollection<Long>> {
        @Override
        public PCollection<Long> expand(
                PCollection<BadRecord> input) {
            if (input.isBounded() == IsBounded.BOUNDED) {
                return input.apply("Combine", Combine.globally(Count.<BadRecord>combineFn()));
            } else {
                return input
                        .apply("Window", Window.into(FixedWindows.of(Duration.standardDays(1))))
                        .apply("Combine", Combine.globally(Count.<BadRecord>combineFn()).withoutDefaults());
            }
        }
    }
}
