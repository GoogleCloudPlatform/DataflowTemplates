package com.google.cloud.teleport.v2.elasticsearch.utils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * ConnectionInformation class used to parse connectionInformation option.
 * ConnectionInformation option can contain URL to Elasticsearch host or CloudId.
 */
public class ConnectionInformation {
    private String elasticsearchURL;
    private String kibanaURL;
    private String kibanaHost;
    private Type   type;

    public String getElasticsearchURL() {
        return elasticsearchURL;
    }

    public void setElasticsearchURL(String elasticsearchURL) {
        this.elasticsearchURL = elasticsearchURL;
    }

    public String getKibanaURL() {
        return kibanaURL;
    }

    public void setKibanaURL(String kibanaURL) {
        this.kibanaURL = kibanaURL;
    }

    public String getKibanaHost() {
        return kibanaHost;
    }

    public void setKibanaHost(String kibanaHost) {
        this.kibanaHost = kibanaHost;
    }

    private Type checkType(String connectionInformation){
        return connectionInformation.toLowerCase().startsWith("http://")
                || connectionInformation.toLowerCase().startsWith("https://")
                ? Type.URL : Type.CLOUD_ID;
    }

    private void parseUrl(String connectionInformation){
        elasticsearchURL = connectionInformation;
    }

    public ConnectionInformation(String connectionInformation){
        if(checkType(connectionInformation).equals(Type.URL)) {
            parseUrl(connectionInformation);
        } else {
            parseCloudId(connectionInformation);
        }
    }

    private void parseCloudId(String connectionInformation){
        String cloudId = "";

        if (connectionInformation.contains(":")) {
            if (connectionInformation.indexOf(":") == connectionInformation.length() - 1) {
                throw new IllegalStateException("cloudId " + connectionInformation + " must begin with a human readable identifier followed by a colon");
            }
            cloudId = connectionInformation.substring(connectionInformation.indexOf(":") + 1);
        }

        String decoded = new String(Base64.getDecoder().decode(cloudId), StandardCharsets.UTF_8);
        String[] decodedParts = decoded.split("\\$");
        if (decodedParts.length != 3) {
            throw new IllegalStateException("cloudId " + connectionInformation + " did not decode to a cluster identifier correctly");
        }

        // domain name and optional port
        String[] domainAndMaybePort = decodedParts[0].split(":", 2);
        String domain = domainAndMaybePort[0];
        int port;

        if (domainAndMaybePort.length == 2) {
            try {
                port = Integer.parseInt(domainAndMaybePort[1]);
            } catch (NumberFormatException nfe) {
                throw new IllegalStateException("cloudId " + connectionInformation + " does not contain a valid port number");
            }
        } else {
            port = 443;
        }

        this.elasticsearchURL = "https://" + decodedParts[1]  + "." + domain + ":" + port;
        this.kibanaURL = "https://" + decodedParts[2]  + "." + domain + ":" + port;
        this.kibanaHost = "https://" + decodedParts[2]  + "." + domain;
    }

    /**
     * Type of ConnectionInformation.
     * **/
    public enum Type {
        URL,
        CLOUD_ID
    }
}
