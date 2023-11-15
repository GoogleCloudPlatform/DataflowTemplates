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
	dRegion         string
	dProject        string
	dArtifactBucket string
	dStageBucket    string
	dHostIp         string
	dReleaseMode    bool
)

// Registers all common flags. Must be called before flag.Parse().
func RegisterItFlags() {
	flag.StringVar(&dRegion, "it-region", "", "The GCP region to use for storing test artifacts")
	flag.StringVar(&dProject, "it-project", "", "The GCP project to run the integration tests in")
	flag.StringVar(&dArtifactBucket, "it-artifact-bucket", "", "A GCP bucket to store test artifacts")
	flag.StringVar(&dStageBucket, "it-stage-bucket", "", "(optional) A GCP bucket to stage templates")
	flag.StringVar(&dHostIp, "it-host-ip", "", "(optional) The ip that the gitactions runner is listening on")
	flag.BoolVar(&dReleaseMode, "it-release", false, "(optional) Set if tests are being executed for a release")
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

func FailureMode() string {
	// Fail releases fast
	if dReleaseMode {
		return "-ff"
	}

	// Fail PRs at the end
	return "-fae"
}

