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

package main

import (
	"flag"
	"log"

	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/flags"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/workflows"
)

func main() {
	flags.RegisterCommonFlags()
	flag.Parse()

	mvnFlags := workflows.NewMavenFlags()
	err := workflows.MvnCleanVerify().Run(
		mvnFlags.IncludeDependencies(),
		mvnFlags.IncludeDependents(),
		mvnFlags.ForceUpdates(),
		mvnFlags.SkipCheckstyle(),
		mvnFlags.SkipJib(),
		mvnFlags.SkipShade(),
		mvnFlags.SkipSpotlessCheck(),
		mvnFlags.SkipIntegrationTests(),
		mvnFlags.FailAtTheEnd(),
		mvnFlags.ThreadCount(8))
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	log.Println("Verification Successful!")
}
