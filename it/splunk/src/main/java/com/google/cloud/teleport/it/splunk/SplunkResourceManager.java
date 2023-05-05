/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.it.splunk;

import com.google.cloud.teleport.it.common.ResourceManager;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.splunk.SplunkEvent;

/** Interface for managing Splunk resources in integration tests. */
public interface SplunkResourceManager extends ResourceManager {

  /**
   * Returns the HTTP endpoint that this Splunk server is configured to listen on.
   *
   * @return the HTTP endpoint.
   */
  String getHttpEndpoint();

  /**
   * Returns the HTTP Event Collector (HEC) endpoint that this Splunk server is configured to
   * receive events at.
   *
   * <p>This will be the HTTP endpoint concatenated with <code>'/services/collector/event'</code>.
   *
   * @return the HEC service endpoint.
   */
  String getHecEndpoint();

  /**
   * Returns the Splunk Http Event Collector (HEC) authentication token used to connect to this
   * Splunk instance's HEC service.
   *
   * @return the HEC authentication token.
   */
  String getHecToken();

  /**
   * Sends the given HTTP event to the Splunk Http Event Collector (HEC) service.
   *
   * <p>Note: Setting the <code>index</code> field in the Splunk event requires the index already
   * being configured in the Splunk instance. Unless using a static Splunk instance, omit this field
   * from the event.
   *
   * @param event The SpunkEvent to send to the HEC service.
   * @return True, if the request was successful.
   */
  boolean sendHttpEvent(SplunkEvent event);

  /**
   * Sends the given HTTP events to the Splunk Http Event Collector (HEC) service.
   *
   * <p>Note: Setting the <code>index</code> field in the Splunk event requires the index already
   * being configured in the Splunk instance. Unless using a static Splunk instance, omit this field
   * from the events.
   *
   * @param event The SpunkEvents to send to the HEC service.
   * @return True, if the request was successful.
   */
  boolean sendHttpEvents(Collection<SplunkEvent> event);

  /**
   * Return a list of Splunk events retrieved from the Splunk server based on the given query.
   *
   * <p>e.g. query: <code>'search source=mySource sourcetype=mySourceType host=myHost'</code>
   *
   * @param query The query to filter events by.
   * @return
   */
  List<SplunkEvent> getEvents(String query);

  /**
   * Return a list of all Splunk events retrieved from the Splunk server.
   *
   * @return All Splunk events on the server.
   */
  List<SplunkEvent> getEvents();
}
