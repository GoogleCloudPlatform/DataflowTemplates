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

package workflows

import (
	"strconv"

	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/op"
)

const (
	// mvn commands
	cleanInstallCmd    = "clean install"
	cleanVerifyCmd     = "clean verify"
	cleanTestCmd       = "clean test"
	verifyCmd          = "verify"
	spotlessCheckCmd   = "spotless:check"
	checkstyleCheckCmd = "checkstyle:check"

	// notable files
	unifiedPom = "pom.xml"
)

// Interface for retrieving flags that can be passed into the workflow's
// `Run` method.
type MavenFlags interface {
	IncludeDependencies() string
	IncludeDependents() string
	SkipCheckstyle() string
	SkipDependencyAnalysis() string
	SkipJib() string
	SkipTests() string
	SkipJacoco() string
	SkipShade() string
	SkipSpotlessCheck() string
	SkipIntegrationTests() string
	FailAtTheEnd() string
	RunIntegrationTests() string
	RunIntegrationSmokeTests() string
	RunLoadTests() string
	RunLoadTestObserver() string
	ThreadCount(int) string
	IntegrationTestParallelism(int) string
	StaticBigtableInstance(string) string
	StaticSpannerInstance(string) string
	SpannerHost(string) string
}

type mvnFlags struct{}

func (*mvnFlags) IncludeDependencies() string {
	return "-am"
}

func (*mvnFlags) IncludeDependents() string {
	return "-amd"
}

func (*mvnFlags) SkipCheckstyle() string {
	return "-Dcheckstyle.skip"
}

func (*mvnFlags) SkipDependencyAnalysis() string {
	return "-Dmdep.analyze.skip"
}

func (*mvnFlags) SkipJib() string {
	return "-Djib.skip"
}

func (*mvnFlags) SkipTests() string {
	return "-Dmaven.test.skip"
}

func (*mvnFlags) SkipJacoco() string {
	return "-Djacoco.skip"
}

func (*mvnFlags) SkipShade() string {
	return "-DskipShade"
}

func (*mvnFlags) SkipSpotlessCheck() string {
	return "-Dspotless.check.skip"
}

func (*mvnFlags) SkipIntegrationTests() string {
	return "-DskipIntegrationTests"
}

func (*mvnFlags) FailAtTheEnd() string {
	return "-fae"
}

func (*mvnFlags) RunIntegrationTests() string {
	return "-PtemplatesIntegrationTests"
}

func (*mvnFlags) RunIntegrationSmokeTests() string {
	return "-PtemplatesIntegrationSmokeTests"
}

func (*mvnFlags) RunLoadTests() string {
	return "-PtemplatesLoadTests"
}

func (*mvnFlags) RunLoadTestObserver() string {
	return "-PtemplatesLoadTestObserve"
}

// The number of modules Maven is going to build in parallel in a multi-module project.
func (*mvnFlags) ThreadCount(count int) string {
	return "-T" + strconv.Itoa(count)
}

// The number of tests Maven Surefire plugin is going to run in parallel for each Maven build
// thread. The total number of parallel tests is IntegrationTestParallelism * ThreadCount.
func (*mvnFlags) IntegrationTestParallelism(count int) string {
	return "-DitParallelism=" + strconv.Itoa(count)
}

func (*mvnFlags) StaticBigtableInstance(instanceID string) string {
	return "-DbigtableInstanceId=" + instanceID
}

func (*mvnFlags) StaticSpannerInstance(instanceID string) string {
	return "-DspannerInstanceId=" + instanceID
}

func (*mvnFlags) SpannerHost(host string) string {
	return "-DspannerHost=" + host
}

func NewMavenFlags() MavenFlags {
	return &mvnFlags{}
}

type mvnCleanInstallWorkflow struct{}

func MvnCleanInstall() Workflow {
	return &mvnCleanInstallWorkflow{}
}

func (*mvnCleanInstallWorkflow) Run(args ...string) error {
	return RunForChangedModules(cleanInstallCmd, args...)
}

type mvnCleanInstallAllWorkflow struct{}

func MvnCleanInstallAll() Workflow {
	return &mvnCleanInstallAllWorkflow{}
}

func (*mvnCleanInstallAllWorkflow) Run(args ...string) error {
	return op.RunMavenOnPom(unifiedPom, cleanInstallCmd, args...)
}

type mvnCleanTestWorkflow struct{}

func MvnCleanTest() Workflow {
	return &mvnCleanTestWorkflow{}
}

func (*mvnCleanTestWorkflow) Run(args ...string) error {
	return RunForChangedModules(cleanTestCmd, args...)
}

type mvnCleanVerifyWorkflow struct{}

func MvnCleanVerify() Workflow {
	return &mvnCleanVerifyWorkflow{}
}

func (*mvnCleanVerifyWorkflow) Run(args ...string) error {
	return RunForChangedModules(cleanVerifyCmd, args...)
}

type mvnVerifyWorkflow struct{}

func MvnVerify() Workflow {
	return &mvnVerifyWorkflow{}
}

func (*mvnVerifyWorkflow) Run(args ...string) error {
	return RunForChangedModules(verifyCmd, args...)
}

func RunForChangedModules(cmd string, args ...string) error {
	parsedArgs := []string{}
	for _, arg := range args {
		if arg != "" {
			parsedArgs = append(parsedArgs, arg)
		}
	}
	return op.RunMavenOnModule(unifiedPom, cmd, parsedArgs...)
}

type spotlessCheckWorkflow struct{}

func SpotlessCheck() Workflow {
	return &spotlessCheckWorkflow{}
}

func (*spotlessCheckWorkflow) Run(args ...string) error {
	return op.RunMavenOnPom(unifiedPom, spotlessCheckCmd, args...)
}

type checkstyleCheckWorkflow struct{}

func CheckstyleCheck() Workflow {
	return &checkstyleCheckWorkflow{}
}

func (*checkstyleCheckWorkflow) Run(args ...string) error {
	return op.RunMavenOnPom(unifiedPom, checkstyleCheckCmd, args...)
}

// Removes root and returns results. This may reorder the input.
func removeRoot(modules []string) []string {
	var i int
	for i = 0; i < len(modules); i += 1 {
		if modules[i] == "" {
			break
		}
	}

	if i == len(modules) {
		return modules
	}

	// Order doesn't matter when passing the modules
	l := len(modules)
	modules[i] = modules[l-1]
	return modules[:l-1]
}
