/*
 * Copyright (C) 2024 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.example.pipeline;

import java.util.Optional;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/** */
public interface ElasticsearchConnectionOptions extends PipelineOptions {

  @Description("Elasticsearch addresses as a comma separated list")
  @Validation.Required
  String getEsAddresses();

  void setEsAddresses(String value);

  @Description("Elasticsearch index to read data from")
  @Validation.Required
  String getEsIndex();

  void setEsIndex(String value);

  @Description("A provided query")
  @Default.String("")
  String getQuery();

  void setQuery(String value);

  @Description("Elasticsearch API key to authenticate")
  String getEsApiKey();

  void setEsApiKey(String value);

  @Description("Elasticsearch user name")
  String getEsUserName();

  void setEsUserName(String value);

  @Description("Elasticsearch user password")
  String getEsUserPassword();

  void setEsUserPassword(String value);

  @Default.InstanceFactory(ESAddresses.class)
  String[] getESAddressesArray();

  void setESAddressesArray(String[] value);

  static class ESAddresses implements DefaultValueFactory<String[]> {

    @Override
    public String[] create(PipelineOptions options) {
      return options.as(ElasticsearchConnectionOptions.class).getEsAddresses().split(",");
    }
  }

  static ElasticsearchIO.ConnectionConfiguration connectionConfigurationFromOptions(
      PipelineOptions options) {
    var esOptions = options.as(ElasticsearchConnectionOptions.class);
    var config =
        ElasticsearchIO.ConnectionConfiguration.create(
            esOptions.getESAddressesArray(), esOptions.getEsIndex());
    return Optional.of(esOptions.getEsApiKey())
        .filter(key -> !key.isBlank())
        .map(key -> config.withApiKey(key))
        .orElseGet(
            () ->
                config
                    .withUsername(esOptions.getEsUserName())
                    .withPassword(esOptions.getEsUserPassword()));
  }
}
