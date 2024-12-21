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

package flags

import (
	"flag"
	"fmt"
	"os/exec"
)

// Avoid making these vars public.
var (
	dRegion                             string
	dProject                            string
	dArtifactBucket                     string
	dStageBucket                        string
	dHostIp                             string
	dPrivateConnectivity                string
	dSpannerHost                        string
	dReleaseMode                        bool
	dRetryFailures                      string
	dCloudProxyHost                     string
	dCloudProxyMySqlPort                string
	dCloudProxyPostgresPort             string
	dCloudProxyPassword                 string
	dOracleHost                         string
	dCloudOracleSysPassword             string
	dUnifiedWorkerHarnessContainerImage string
)

// Registers all it flags. Must be called before flag.Parse().
func RegisterItFlags() {
	flag.StringVar(&dRegion, "it-region", "", "The GCP region to use for storing test artifacts")
	flag.StringVar(&dProject, "it-project", "", "The GCP project to run the integration tests in")
	flag.StringVar(&dArtifactBucket, "it-artifact-bucket", "", "A GCP bucket to store test artifacts")
	flag.StringVar(&dStageBucket, "it-stage-bucket", "", "(optional) A GCP bucket to stage templates")
	flag.StringVar(&dHostIp, "it-host-ip", "", "(optional) The ip that the gitactions runner is listening on")
	flag.StringVar(&dPrivateConnectivity, "it-private-connectivity", "", "(optional) A GCP private connectivity endpoint")
	flag.StringVar(&dSpannerHost, "it-spanner-host", "", "(optional) A custom endpoint to override Spanner API requests")
	flag.BoolVar(&dReleaseMode, "it-release", false, "(optional) Set if tests are being executed for a release")
	flag.StringVar(&dRetryFailures, "it-retry-failures", "0", "Number of retries attempts for failing tests")
	flag.StringVar(&dCloudProxyHost, "it-cloud-proxy-host", "10.128.0.34", "Hostname or IP address of static Cloud Auth Proxy")
	flag.StringVar(&dCloudProxyMySqlPort, "it-cloud-proxy-mysql-port", "33134", "MySql port number on static Cloud Auth Proxy")
	flag.StringVar(&dCloudProxyPostgresPort, "it-cloud-proxy-postgres-port", "33136", "Postgres port number on static Cloud Auth Proxy")
	flag.StringVar(&dCloudProxyPassword, "it-cloud-proxy-password", "t>5xl%J(&qTK6?FaZ", "Password of static Cloud Auth Proxy")
	flag.StringVar(&dOracleHost, "it-oracle-host", "10.128.0.90", "Hostname or IP address of static Oracle DB")
	flag.StringVar(&dCloudOracleSysPassword, "it-oracle-sys-password", "oracle", "sys password of static Oracle DB")
	flag.StringVar(&dUnifiedWorkerHarnessContainerImage, "it-unified-worker-harness-container-image", "", "Runner harness image to run tests against")
}

func Region() string {
	return "-Dregion=" + dRegion
}

func Project() string {
	return "-Dproject=" + dProject
}

func ArtifactBucket() string {
	return "-DartifactBucket=" + dArtifactBucket
}

func StageBucket() string {
	if dStageBucket == "" {
		return "-DstageBucket=" + dArtifactBucket
	}
	return "-DstageBucket=" + dStageBucket
}

func HostIp() string {
	if len(dHostIp) == 0 {
		gcloudCmd := "gcloud compute instances list | grep $(hostname) | awk '{print $4}'"
		if hostIP, err := exec.Command("bash", "-c", gcloudCmd).Output(); err != nil || len(hostIP) == 0 {
			panic(fmt.Errorf("failed to get gitactions runner host ip: %v", err))
		} else {
			return "-DhostIp=" + string(hostIP)[:len(hostIP)-1]
		}
	}
	return "-DhostIp=" + dHostIp
}

func PrivateConnectivity() string {
	if dPrivateConnectivity != "" {
		return "-DprivateConnectivity=" + dPrivateConnectivity
	}
	return ""
}

func SpannerHost() string {
	if dSpannerHost != "" {
		return "-DspannerHost=" + dSpannerHost
	}
	return ""
}

func FailureMode() string {
	// Fail releases fast
	if dReleaseMode {
		return "-ff"
	}

	// Fail PRs at the end
	return "-fae"
}

func RetryFailures() string {
	return "-Dsurefire.rerunFailingTestsCount=" + dRetryFailures
}

func CloudProxyHost() string {
	return "-DcloudProxyHost=" + dCloudProxyHost
}

func CloudProxyMySqlPort() string {
	return "-DcloudProxyMySqlPort=" + dCloudProxyMySqlPort
}

func CloudProxyPostgresPort() string {
	return "-DcloudProxyPostgresPort=" + dCloudProxyPostgresPort
}

func CloudProxyPassword() string {
	return "-DcloudProxyPassword=" + dCloudProxyPassword
}

func StaticOracleHost() string {
	return "-DcloudOracleHost=" + dOracleHost
}

func StaticOracleSysPassword() string {
	return "-DcloudOracleSysPassword=" + dCloudOracleSysPassword
}

func UnifiedWorkerHarnessContainerImage() string {
	if dUnifiedWorkerHarnessContainerImage != "" {
		return "-DunifiedWorkerHarnessContainerImage=" + dUnifiedWorkerHarnessContainerImage
	}
	return ""
}
