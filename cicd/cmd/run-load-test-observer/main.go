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

package main

import (
	"flag"
	"log"

	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/flags"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/workflows"
)

func main() {
	flags.RegisterCommonFlags()
	flags.RegisterItFlags()
	flags.RegisterLtFlags()
	flag.Parse()

	// Run mvn install before running integration tests
	mvnFlags := workflows.NewMavenFlags()
	err := workflows.MvnCleanInstall().Run(
		mvnFlags.IncludeDependencies(),
		mvnFlags.IncludeDependents(),
		mvnFlags.SkipDependencyAnalysis(),
		mvnFlags.SkipCheckstyle(),
		mvnFlags.SkipJib(),
		mvnFlags.SkipTests(),
		mvnFlags.SkipJacoco(),
		mvnFlags.SkipShade(),
		mvnFlags.ThreadCount(4))
	if err != nil {
		log.Fatalf("%v\n", err)
	}

	// Run load tests
	mvnFlags = workflows.NewMavenFlags()
	err = workflows.MvnVerify().Run(
		mvnFlags.IncludeDependencies(),
		mvnFlags.IncludeDependents(),
		mvnFlags.SkipDependencyAnalysis(),
		mvnFlags.SkipCheckstyle(),
		mvnFlags.SkipJib(),
		mvnFlags.SkipShade(),
		mvnFlags.FailAtTheEnd(),
		mvnFlags.RunLoadTestObserver(),
		mvnFlags.ThreadCount(4),
		flags.Region(),
		flags.Project(),
		flags.ArtifactBucket(),
		flags.StageBucket(),
		flags.HostIp(),
		flags.PrivateConnectivity(),
		flags.ExportProject(),
		flags.ExportDataset(),
		flags.ExportTable(),
		flags.StaticOracleHost(),
		flags.StaticOracleSysPassword(),
		flags.CloudProxyHost(),
		flags.CloudProxyMySqlPort(),
		flags.CloudProxyPostgresPort(),
		flags.CloudProxyPassword())
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	log.Println("Build Successful!")
}
