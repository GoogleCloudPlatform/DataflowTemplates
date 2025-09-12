/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.compute.v1.Denied;
import com.google.cloud.compute.v1.Firewall;
import com.google.cloud.compute.v1.FirewallsClient;
import com.google.cloud.compute.v1.Operation;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkFailureInjector {

  private static final Logger LOG = LoggerFactory.getLogger(NetworkFailureInjector.class);

  /**
   * Blocks network connectivity for a host with a tag in a VPC network for a specific port by
   * adding a firewall rule for a few seconds.
   *
   * @param projectId The Google Cloud project ID.
   * @param networkName The VPC network name.
   * @param targetTag The tag of the host to block network connectivity for.
   * @param port The port to block.
   * @param duration The duration to block network connectivity for.
   */
  public static void blockNetworkForHost(
      String projectId, String networkName, String targetTag, int port, Duration duration)
      throws IOException, ExecutionException, InterruptedException {

    try (FirewallsClient firewallsClient = FirewallsClient.create()) {
      String ruleId = "network-failure-" + UUID.randomUUID();
      String ingressFirewallRuleName = ruleId + "-ingress";
      String egressFirewallRuleName = ruleId + "-egress";
      String networkUrl = String.format("projects/%s/global/networks/%s", projectId, networkName);

      Denied deniedRule =
          Denied.newBuilder().setIPProtocol("tcp").addPorts(String.valueOf(port)).build();

      Firewall ingressFirewallRule =
          Firewall.newBuilder()
              .setName(ingressFirewallRuleName)
              .setNetwork(networkUrl)
              .setDirection("INGRESS")
              .setPriority(100) // High priority
              .addAllDenied(Collections.singletonList(deniedRule))
              .addAllTargetTags(Collections.singletonList(targetTag))
              .addAllSourceRanges(Collections.singletonList("0.0.0.0/0"))
              .build();

      Firewall egressFirewallRule =
          Firewall.newBuilder()
              .setName(egressFirewallRuleName)
              .setNetwork(networkUrl)
              .setDirection("EGRESS")
              .setPriority(100) // High priority
              .addAllDenied(Collections.singletonList(deniedRule))
              .addAllTargetTags(Collections.singletonList(targetTag))
              .addAllDestinationRanges(Collections.singletonList("0.0.0.0/0"))
              .build();

      LOG.info(
          "Injecting network failure by creating firewall rules '{}' and '{}' to block port {} for tag '{}'...",
          ingressFirewallRuleName,
          egressFirewallRuleName,
          port,
          targetTag);

      OperationFuture<Operation, Operation> insertIngressOperationFuture =
          firewallsClient.insertAsync(projectId, ingressFirewallRule);
      OperationFuture<Operation, Operation> insertEgressOperationFuture =
          firewallsClient.insertAsync(projectId, egressFirewallRule);

      Operation insertIngressResponse = insertIngressOperationFuture.get();
      Operation insertEgressResponse = insertEgressOperationFuture.get();

      if (insertIngressResponse.hasError()) {
        LOG.error(
            "Error creating firewall rule '{}': {}",
            ingressFirewallRuleName,
            insertIngressResponse.getError());
        // Clean up egress rule if ingress failed
        firewallsClient.deleteAsync(projectId, egressFirewallRuleName).get();
        throw new RuntimeException(
            "Failed to create ingress firewall rule: "
                + insertIngressResponse.getError().toString());
      }
      if (insertEgressResponse.hasError()) {
        LOG.error(
            "Error creating firewall rule '{}': {}",
            egressFirewallRuleName,
            insertEgressResponse.getError());
        // Clean up ingress rule if egress failed
        firewallsClient.deleteAsync(projectId, ingressFirewallRuleName).get();
        throw new RuntimeException(
            "Failed to create egress firewall rule: " + insertEgressResponse.getError().toString());
      }

      LOG.info(
          "-> Firewall rules '{}' and '{}' created successfully.",
          ingressFirewallRuleName,
          egressFirewallRuleName);

      try {
        LOG.info("Sleeping for {} seconds to simulate network partition...", duration.getSeconds());
        Thread.sleep(duration.toMillis());
      } finally {
        LOG.info(
            "-> Removing firewall rules '{}' and '{}' to restore connectivity...",
            ingressFirewallRuleName,
            egressFirewallRuleName);
        OperationFuture<Operation, Operation> deleteIngressOperationFuture =
            firewallsClient.deleteAsync(projectId, ingressFirewallRuleName);
        OperationFuture<Operation, Operation> deleteEgressOperationFuture =
            firewallsClient.deleteAsync(projectId, egressFirewallRuleName);

        Operation deleteIngressResponse = deleteIngressOperationFuture.get();
        Operation deleteEgressResponse = deleteEgressOperationFuture.get();

        if (deleteEgressResponse.hasError()) {
          LOG.error(
              "Error deleting firewall rule '{}': {}",
              egressFirewallRuleName,
              deleteEgressResponse.getError());
        } else {
          LOG.info("-> Firewall rule '{}' deleted successfully.", egressFirewallRuleName);
        }
        if (deleteIngressResponse.hasError()) {
          LOG.error(
              "Error deleting firewall rule '{}': {}",
              ingressFirewallRuleName,
              deleteIngressResponse.getError());
        } else {
          LOG.info("-> Firewall rule '{}' deleted successfully.", ingressFirewallRuleName);
        }
      }
    }
  }
}
