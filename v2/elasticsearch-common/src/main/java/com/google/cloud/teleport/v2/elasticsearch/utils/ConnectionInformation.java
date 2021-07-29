/*
 * Copyright (C) 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.elasticsearch.utils;

import com.google.common.net.InetAddresses;
import com.google.common.net.InternetDomainName;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ConnectionInformation class used to parse connectionInformation option.
 * ConnectionInformation option can contain URL to Elasticsearch host or CloudId.
 */
public class ConnectionInformation {
    private URL elasticsearchURL;
    private URL kibanaURL;
    private Type type;

    public URL getElasticsearchURL() {
        return elasticsearchURL;
    }

    public URL getKibanaURL() {
        return kibanaURL;
    }

    public Type getType() {
        return type;
    }

    private static boolean isValidUrlFormat(String url) {
        Pattern pattern = Pattern.compile("^http(s?)://([^:]+)(:[0-9]+)?$");
        Matcher matcher = pattern.matcher(url);
        if (matcher.find()) {
            String host = matcher.group(2);
            return InetAddresses.isInetAddress(host) || InternetDomainName.isValid(host);
        }
        return false;
    }

    public ConnectionInformation(String connectionInformation){
        try {
            parse(connectionInformation);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("ConnectionUrl has invalid value", e);
        }
    }

    private void parse(String connectionInformation) throws MalformedURLException {
        String cloudId = "";

        if (isValidUrlFormat(connectionInformation)) {
            elasticsearchURL = new URL(connectionInformation);
            type = Type.URL;
        } else {
            type = Type.CLOUD_ID;

            if (connectionInformation.contains(":")) {
                if (connectionInformation.indexOf(":") == connectionInformation.length() - 1) {
                    throw new IllegalStateException("cloudId " + connectionInformation
                            + " must begin with a human readable identifier followed by a colon");
                }
                cloudId = connectionInformation.substring(connectionInformation.indexOf(":") + 1);
            }

            String decoded = new String(Base64.getDecoder().decode(cloudId), StandardCharsets.UTF_8);
            String[] decodedParts = decoded.split("\\$");
            if (decodedParts.length != 3) {
                throw new IllegalStateException("cloudId " + connectionInformation
                        + " did not decode to a cluster identifier correctly");
            }

            String elasticsearchHost = decodedParts[1];
            String kibanaHost = decodedParts[2];

            // domain name and optional port
            String[] domainAndMaybePort = decodedParts[0].split(":", 2);
            String domain = domainAndMaybePort[0];
            int port;

            if (domainAndMaybePort.length == 2) {
                try {
                    port = Integer.parseInt(domainAndMaybePort[1]);
                } catch (NumberFormatException nfe) {
                    throw new IllegalStateException("cloudId " + connectionInformation
                            + " does not contain a valid port number");
                }
            } else {
                port = 443;
            }

            this.elasticsearchURL = new URL(String.format("https://%s.%s:%d", elasticsearchHost, domain, port));
            this.kibanaURL = new URL(String.format("https://%s.%s:%d", kibanaHost, domain, port));
        }
    }

    /**
     * Type of ConnectionInformation.
     * **/
    public enum Type {
        URL,
        CLOUD_ID
    }
}
