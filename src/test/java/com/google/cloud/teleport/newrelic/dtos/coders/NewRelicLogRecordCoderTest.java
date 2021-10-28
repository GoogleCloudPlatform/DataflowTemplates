package com.google.cloud.teleport.newrelic.dtos.coders;

import com.google.cloud.teleport.newrelic.dtos.NewRelicLogRecord;
import org.apache.beam.sdk.coders.CoderException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class NewRelicLogRecordCoderTest {
    private static final LocalDateTime SOME_DATE_TIME = LocalDateTime.of(1987, Month.AUGUST, 8, 21, 0, 0);
    private static final String PAYLOAD = "{\"message\": \"MY MESSAGE\", \"timestamp\": \"" + SOME_DATE_TIME.toString() + "\"}";
    // No need to define another message for these tests, but I did for consistency and clarity (documentation purposes)
    private static final String PAYLOAD_WITHOUT_TIMESTAMP = "{\"message\": \"MY MESSAGE\"}";
    private static final long TIMESTAMP = SOME_DATE_TIME.toInstant(ZoneOffset.UTC).toEpochMilli();

    @Test
    public void shouldCodeAndDecodeIntoAnEqualObject() throws IOException {
        final NewRelicLogRecord original = new NewRelicLogRecord(PAYLOAD, TIMESTAMP);

        final PipedInputStream input = new PipedInputStream();
        // Whatever is written to this output stream will be readable from the input one.
        final PipedOutputStream output = new PipedOutputStream(input);

        NewRelicLogRecordCoder.getInstance().encode(original, output);
        final NewRelicLogRecord decoded = NewRelicLogRecordCoder.getInstance().decode(input);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    public void shouldWorkWellWithNullTimestamp() throws IOException {
        final NewRelicLogRecord original = new NewRelicLogRecord(PAYLOAD_WITHOUT_TIMESTAMP, null);

        final PipedInputStream input = new PipedInputStream();
        // Whatever is written to this output stream will be readable from the input one.
        final PipedOutputStream output = new PipedOutputStream(input);

        NewRelicLogRecordCoder.getInstance().encode(original, output);
        final NewRelicLogRecord decoded = NewRelicLogRecordCoder.getInstance().decode(input);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    public void shouldFailWithNullMessage() throws IOException {
        final NewRelicLogRecord original = new NewRelicLogRecord(null, null);

        assertThrows(CoderException.class, () ->
                NewRelicLogRecordCoder.getInstance().encode(original, mock(OutputStream.class))
        );
    }
}