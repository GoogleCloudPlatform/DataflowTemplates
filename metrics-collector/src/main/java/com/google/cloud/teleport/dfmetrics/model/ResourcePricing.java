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
package com.google.cloud.teleport.dfmetrics.model;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class {@link ResourcePricing} represents the pricing associated for various resources consumed by
 * the job.
 */
public class ResourcePricing {
  private static final Logger LOG = LoggerFactory.getLogger(ResourcePricing.class);
  private double vcpuPerHour;
  private double memoryGbPerHour;
  private double pdGbPerHour;
  private double ssdGbPerHour;
  private double shuffleDataPerGb;
  private double streamingDataPerGb;

  private static final int SECONDS_PER_HOUR = 3600;
  private static final int GB_IN_BYTES = 1024 * 1024 * 1024;

  private static double scale = Math.pow(10, 3);

  public double getVcpuPerHour() {
    return vcpuPerHour;
  }

  public void setVcpuPerHour(double vcpuPerHour) {
    this.vcpuPerHour = vcpuPerHour;
  }

  public double getMemoryGbPerHour() {
    return memoryGbPerHour;
  }

  public void setMemoryGbPerHour(double memoryGbPerHour) {
    this.memoryGbPerHour = memoryGbPerHour;
  }

  public double getPdGbPerHour() {
    return pdGbPerHour;
  }

  public void setPdGbPerHour(double pdGbPerHour) {
    this.pdGbPerHour = pdGbPerHour;
  }

  public double getSsdGbPerHour() {
    return ssdGbPerHour;
  }

  public void setSsdGbPerHour(double ssdGbPerHour) {
    this.ssdGbPerHour = ssdGbPerHour;
  }

  public double getshuffleDataPerGb() {
    return shuffleDataPerGb;
  }

  public void setshuffleDataPerGb(double shuffleDataPerGb) {
    this.shuffleDataPerGb = shuffleDataPerGb;
  }

  public double getstreamingDataPerGb() {
    return streamingDataPerGb;
  }

  public void setstreamingDataPerGb(double streamingDataPerGb) {
    this.streamingDataPerGb = streamingDataPerGb;
  }

  private double getPrice(
      Map<String, Double> metrics, String metricName, int denominator, double price) {
    return ((metrics.getOrDefault(metricName, 0.0)) / denominator) * price;
  }

  /**
   * Calculates estimated job cost.
   *
   * @param metrics
   * @return
   */
  public double estimateJobCost(Map<String, Double> metrics) {
    // Check if dataflow prime is enabled. If so return the cos
    double dcuPrice = getPrice(metrics, "TotalDcuUsage", 1, 1);
    if (dcuPrice > 0) {
      LOG.warn("Estimated Job Cost is not available for prime enabled jobs");
      return 0.0;
    }

    double estimatedCost =
        getPrice(metrics, "TotalVcpuTime", SECONDS_PER_HOUR, vcpuPerHour)
            + getPrice(metrics, "TotalMemoryUsage", (SECONDS_PER_HOUR * 1024), memoryGbPerHour)
            + getPrice(metrics, "TotalPdUsage", SECONDS_PER_HOUR, pdGbPerHour)
            + getPrice(metrics, "TotalSsdUsage", SECONDS_PER_HOUR, ssdGbPerHour)
            + getPrice(metrics, "BillableShuffleDataProcessed", GB_IN_BYTES, shuffleDataPerGb);

    return Math.round(estimatedCost * scale) / scale;
  }
}
