/*
 * Copyright (C) 2024 Google LLC
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
)

// Avoid making these vars public.
var (
	dBucketName          string
	dLibrariesBucketName string
	dStagePrefix         string
)

// Registers all release flags. Must be called before flag.Parse().
func RegisterReleaseFlags() {
	flag.StringVar(&dBucketName, "release-bucket-name", "", "The GCP bucket to stage the released templates.")
	flag.StringVar(&dLibrariesBucketName, "release-libraries-bucket-name", "", "The GCP bucket to stage the released template libraries.")
	flag.StringVar(&dStagePrefix, "release-stage-prefix", "", "Prefix to use as parent folder in GCS for released templates.")
}

func BucketName() string {
	return "-DbucketName=" + dBucketName
}

func LibrariesBucketName() string {
	return "-DlibrariesBucketName=" + dLibrariesBucketName
}

func StagePrefix() string {
	return "-DstagePrefix=" + dStagePrefix
}
