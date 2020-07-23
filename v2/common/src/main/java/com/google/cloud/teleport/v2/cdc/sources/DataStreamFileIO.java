/*
 * Copyright (C) 2018 Google Inc.
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

package com.google.cloud.teleport.v2.cdc.sources;

import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.GcsUtilFactory;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class designed to inspect the directory tree produced by Cloud DataStream.
 *
 * This class can work in continuous streaming mode or in single-check batch mode.
 *
 * In streaming mode, every 100 seconds, this transform looks for new "objects", where each
 * object represents a table. 100 seconds was chosen to avoid performing this operation
 * too often, as it requires listing the full directory tree, which is very costly.
 *
 * An object represents a new directory at the base of the root path:
 * <ul>
 *   <li>`gs://BUCKET/`</li>
 *   <li>`gs://BUCKET/root/prefix/`</li>
 *   <li>`gs://BUCKET/root/prefix/HR_JOBS/` - This directory represents an "object"</li>
 *   <li>`gs://BUCKET/root/prefix/HR_SALARIES/` - This directory represents an "object"</li>
 * </ul>
 *
 * Every time a new "object" is discovered, a new key is created for it, and new directories
 * are monitored. Monitoring of directories is done periodically with the `matchPeriod`
 * parameter.
 *
 * <ul>
 *    <li>`gs://BUCKET/root/prefix/HR_JOBS/` - This directory represents an "object"</li>
 *    <li>`gs://BUCKET/root/prefix/HR_JOBS/2020/07/14/11/03/` - This directory is an example
 *  *    of the final output of this transform.</li>
 *    <li>`gs://BUCKET/root/prefix/HR_JOBS/2020/07/14/12/35/` - This directory is an example
 *  *    of the final output of this transform.</li>
 *    <li>`gs://BUCKET/root/prefix/HR_SALARIES/` - This directory represents an "object"</li>
 *  </ul>
 */
public class DataStreamFileIO extends PTransform<PCollection<String>, PCollection<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamFileIO.class);

  private final String rootPath;
  private final Boolean continuousMatch;
  private final Duration matchPeriod;

  public static DataStreamFileIO matchRootPath(String rootPath) {
    return new DataStreamFileIO(rootPath, false, null);
  }

  public static DataStreamFileIO matchRootPathPeriodically(String rootPath, Duration matchPeriod) {
    return new DataStreamFileIO(rootPath, true, matchPeriod);
  }

  DataStreamFileIO(String rootPath, Boolean continuousMatch, Duration matchPeriod) {
    this.rootPath = rootPath;
    this.continuousMatch = continuousMatch;
    this.matchPeriod = matchPeriod;
  }

  @Override
  public PCollection<String> expand(PCollection<String> start) {
    PCollection<KV<String, String>> impulse;
    if (continuousMatch) {
      impulse =
          start
              .apply(WithKeys.of(any -> rootPath))
              .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
              .apply(ParDo.of(new PerKeyHeartBeat(Duration.standardSeconds(180))))
              .apply(WithKeys.of(elm -> elm))
              .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    } else {
      impulse = start
          .apply(WithKeys.of(any -> rootPath))
          .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    }
    PCollection<String> objectDirectories = impulse
        .apply("MatchDatastreamObjectDirectories",
            ParDo.of(new FindChildrenDirectories(1, 1)));

    PCollection<String> hourlyDirectories;
    if (continuousMatch) {
      hourlyDirectories =
          objectDirectories
              .apply(WithKeys.of(elm -> elm))
              .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
              .apply(ParDo.of(new PerKeyHeartBeat(matchPeriod)))
              .apply(WithKeys.of(elm -> elm))
              .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
              .apply("MatchDateDirectories", ParDo.of(new FindChildrenDirectories(5, 5)));
    } else {
      hourlyDirectories = objectDirectories
          .apply(WithKeys.of(elm -> elm))
          .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
          .apply("MatchDateDirectories",
              ParDo.of(new FindChildrenDirectories(5, 5)));
    }
    return hourlyDirectories;
  }

  static class PerKeyHeartBeat extends DoFn<KV<String, String>, String> {
    private final Duration period;
    PerKeyHeartBeat(Duration period) {
      this.period = period;
    }

    @StateId("key")
    private final StateSpec<ValueState<String>> keyStateSpec =
        StateSpecs.value(StringUtf8Coder.of());

    @TimerId("heartbeat")
    private final TimerSpec heartbeatSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void newKey(
        @Element KV<String, String> newKey,
        @TimerId("heartbeat") Timer heartBeatTimer,
        @StateId("key") ValueState<String> keyState,
        OutputReceiver<String> output) {
      // On time 0 of the key being found, we output the key, and set a timer to 0+period.
      keyState.write(newKey.getKey());
      heartBeatTimer.offset(period).setRelative();
      output.output(newKey.getKey());
    }

    @OnTimer("heartbeat")
    public void onHeartBeat(
        @StateId("key") ValueState<String> keyState,
        @TimerId("heartbeat") Timer heartBeatTimer,
        OnTimerContext c) {
      String key = keyState.read();
      heartBeatTimer.offset(period).setRelative();
      c.output(key);
    }
  }

  /**
   * This class finds children directories for a base directory.
   */
  static class FindChildrenDirectories extends DoFn<KV<String, String>, String> {
    private final Integer minDepth;
    private final Integer maxDepth;

    FindChildrenDirectories(Integer minDepth, Integer maxDepth) {
      this.maxDepth = maxDepth;
      this.minDepth = minDepth;
    }

    private Integer getObjectDepth(String objectName) {
      int depthCount = 1;
      for (char i : objectName.toCharArray()) {
        if (i == '/') {
          depthCount += 1;
        }
      }
      return depthCount;
    }

    private List<String> getMatchingObjects(GcsPath path) throws IOException {
      List<String> result = new ArrayList<>();
      Integer baseDepth = getObjectDepth(path.getObject());
      GcsUtil util = new GcsUtilFactory().create(PipelineOptionsFactory.create());
      String pageToken = null;
      do {
        Objects objects = util.listObjects(path.getBucket(), path.getObject(), pageToken);
        pageToken = objects.getNextPageToken();
        for (StorageObject object : objects.getItems()) {
          String fullName = "gs://" + object.getBucket() + "/" + object.getName();
          if (!object.getName().endsWith("/")) {
            // This object is not a directory, and should be ignored.
            continue;
          }
          if (object.getName().equals(path.getObject())) {
            // Output only direct children and not the directory itself.
            continue;
          }
          Integer newDepth = getObjectDepth(object.getName());
          if (baseDepth + minDepth <= newDepth && newDepth <= baseDepth + maxDepth) {
            result.add(fullName);
          }
        }
      } while (pageToken != null);
      return result;
    }

    // Using BagState as Dataflow does not support SetState
    // TODO(pabloem): Switch to SetState.
    @StateId("knownPaths")
    private final StateSpec<BagState<String>> knownPathsSpec = StateSpecs.bag(StringUtf8Coder.of());

    @ProcessElement
    public void process(
        @Element KV<String, String> baseUriKv,
        OutputReceiver<String> outputReceiver,
        @StateId("knownPaths") BagState<String> knownPathsState)
        throws IOException {
      GcsPath path = GcsPath.fromUri(baseUriKv.getKey());
      List<String> objects = getMatchingObjects(path);

      Set<String> knownPathsRead = Sets.newHashSet(knownPathsState.read());

      objects.forEach(
          gcsObject -> {
            boolean alreadyKnown = knownPathsRead.contains(gcsObject);
            // If the directory is new, we output it.
            if (!alreadyKnown) {
              // We also add it to the known paths.
              knownPathsState.add(gcsObject);
              outputReceiver.output(gcsObject);
            }
          });
    }
  }
}
