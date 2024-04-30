package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.options.KafkaToKafkaOptions;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import java.util.ArrayList;
import java.util.List;

/**
 * Class {@link Credentials} returns the SASL_PLAIN usernames and passwords
 * for Kafka source and destinations.
 */

 final class Credentials{
  public static List<String> accessSecretVersion(KafkaToKafkaOptions options) {
    List<String> saslCredentials = new ArrayList<>();
    String sourceUsername = SecretManagerUtils.getSecret(options.getSourceUsernameVersionId());
    String sourcePassword = SecretManagerUtils.getSecret(options.getSourcePasswordVersionId());
    String destinationUsername = SecretManagerUtils.getSecret(options.getDestinationUsernameVersionId());
    String destinationPassword = SecretManagerUtils.getSecret(options.getDestinationPasswordVersionId());
    saslCredentials.add(sourceUsername);
    saslCredentials.add(sourcePassword);
    saslCredentials.add(destinationUsername);
    saslCredentials.add(destinationPassword);

    return saslCredentials;


  }

}


