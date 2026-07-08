/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.utils;

import com.google.cloud.teleport.v2.neo4j.actions.HttpMethod;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Http client convenience utilities. */
public class HttpUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

  public static CloseableHttpResponse getHttpResponse(
      HttpMethod method, String uri, Map<String, String> headers)
      throws IOException, URISyntaxException {

    try (CloseableHttpClient client = HttpClients.createDefault()) {
      HttpUriRequest request = buildRequest(method, uri);
      for (NameValuePair t : getHeaderPairs(headers)) {
        request.addHeader(t.getName(), t.getValue());
      }
      return client.execute(request);
    }
  }

  public static String getResponseContent(CloseableHttpResponse httpResponse) throws IOException {
    LOG.info("GET Response Status: {}", httpResponse.getStatusLine().getStatusCode());

    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(httpResponse.getEntity().getContent()))) {

      String inputLine;
      StringBuilder response = new StringBuilder();

      while ((inputLine = reader.readLine()) != null) {
        response.append(inputLine);
      }
      try {
        httpResponse.close();
      } catch (Exception e) {
        LOG.error("Exception closing connection.");
      }

      return response.toString();
    }
  }

  private static HttpUriRequest buildRequest(HttpMethod method, String uri)
      throws URISyntaxException {
    return switch (method) {
      case POST -> new HttpPost(uri);
      case GET -> {
        URIBuilder builder = new URIBuilder(uri);
        yield new HttpGet(builder.build());
      }
    };
  }

  private static List<NameValuePair> getHeaderPairs(Map<String, String> headers) {
    if (headers == null) {
      return List.of();
    }
    return headers.entrySet().stream()
        .map(entry -> (NameValuePair) new BasicNameValuePair(entry.getKey(), entry.getValue()))
        .toList();
  }
}
