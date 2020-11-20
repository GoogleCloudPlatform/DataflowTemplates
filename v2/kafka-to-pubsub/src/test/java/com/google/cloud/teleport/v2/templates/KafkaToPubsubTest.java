package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.kafka.consumer.Utils.getKafkaCredentialsFromVault;
import static com.google.cloud.teleport.v2.templates.KafkaPubsubConstants.PASSWORD;
import static com.google.cloud.teleport.v2.templates.KafkaPubsubConstants.USERNAME;

import com.google.cloud.teleport.v2.kafka.consumer.Utils;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.junit.Assert;
import org.junit.Test;


/** Test class for {@link KafkaToPubsub}. */
public class KafkaToPubsubTest {

    /** Tests configureKafka() with a null input properties. */
    @Test
    public void testConfigureKafkaNullProps() {
        Map<String, Object> config = Utils.configureKafka(null);
        Assert.assertEquals(config, new HashMap<>());
    }

    /** Tests configureKafka() without a Password in input properties. */
    @Test
    public void testConfigureKafkaNoPassword() {
        Map<String, String> props = new HashMap<>();
        props.put(USERNAME, "username");
        Map<String, Object> config = Utils.configureKafka(props);
        Assert.assertEquals(config, new HashMap<>());
    }

    /** Tests configureKafka() without a Username in input properties. */
    @Test
    public void testConfigureKafkaNoUsername() {
        Map<String, String> props = new HashMap<>();
        props.put(PASSWORD, "password");
        Map<String, Object> config = Utils.configureKafka(props);
        Assert.assertEquals(config, new HashMap<>());
    }

    /** Tests configureKafka() with an appropriate input properties. */
    @Test
    public void testConfigureKafka() {
        Map<String, String> props = new HashMap<>();
        props.put(USERNAME, "username");
        props.put(PASSWORD, "password");

        Map<String, Object> expectedConfig = new HashMap<>();
        expectedConfig.put(SaslConfigs.SASL_MECHANISM, ScramMechanism.SCRAM_SHA_512.mechanismName());
        expectedConfig.put(
                SaslConfigs.SASL_JAAS_CONFIG,
                String.format(
                        "org.apache.kafka.common.security.scram.ScramLoginModule required "
                                + "username=\"%s\" password=\"%s\";",
                        props.get(USERNAME), props.get(PASSWORD)));

        Map<String, Object> config = Utils.configureKafka(props);
        Assert.assertEquals(config, expectedConfig);
    }

    /** Tests getKafkaCredentialsFromVault() with an invalid url. */
    @Test
    public void testGetKafkaCredentialsFromVaultInvalidUrl() {
        Map<String, Map<String, String>> credentials = getKafkaCredentialsFromVault("some-url", "some-token");
        Assert.assertEquals(credentials, new HashMap<>());
    }
}
