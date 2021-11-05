package com.google.cloud.teleport.newrelic.dtos.coders;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.newrelic.dtos.NewRelicLogApiSendError;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import org.junit.jupiter.api.Test;

class NewRelicLogApiSendErrorCoderTest {

    private static final String PAYLOAD = "{\"message\": \"MY MESSAGE\"}";
    private static final String STATUS_MESSAGE = "Internal Server Error";
    private static final int STATUS_CODE = 500;

    @Test
    public void shouldCodeAndDecodeIntoAnEqualObject() throws IOException {
        final NewRelicLogApiSendError original = new NewRelicLogApiSendError(PAYLOAD, STATUS_MESSAGE, STATUS_CODE);

        final PipedInputStream input = new PipedInputStream();
        // Whatever is written to this output stream will be readable from the input one.
        final PipedOutputStream output = new PipedOutputStream(input);

        NewRelicLogApiSendErrorCoder.getInstance().encode(original, output);
        final NewRelicLogApiSendError decoded = NewRelicLogApiSendErrorCoder.getInstance().decode(input);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    public void shouldWorkWellWithNullValues() throws IOException {
        final NewRelicLogApiSendError original = new NewRelicLogApiSendError(null, null, null);

        final PipedInputStream input = new PipedInputStream();
        // Whatever is written to this output stream will be readable from the input one.
        final PipedOutputStream output = new PipedOutputStream(input);

        NewRelicLogApiSendErrorCoder.getInstance().encode(original, output);
        final NewRelicLogApiSendError decoded = NewRelicLogApiSendErrorCoder.getInstance().decode(input);

        assertThat(decoded).isEqualTo(original);
    }
}