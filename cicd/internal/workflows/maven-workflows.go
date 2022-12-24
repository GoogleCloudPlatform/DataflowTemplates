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
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/flags"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/op"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/repo"
)

const (
	// mvn commands
	cleanInstallCmd  = "clean install"
	cleanVerifyCmd   = "clean verify"
	spotlessCheckCmd = "spotless:check"

	// regexes
	javaFileRegex     = "\\.java$"
	xmlFileRegex      = "\\.xml$"
	markdownFileRegex = "\\.md$"
	pomFileRegex      = "pom\\.xml$"

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
	SkipIntegrationTests() string
	FailAtTheEnd() string
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

func (*mvnFlags) SkipIntegrationTests() string {
	return "-DskipIntegrationTests"
}

func (*mvnFlags) FailAtTheEnd() string {
	return "-fae"
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

type mvnCleanVerifyWorkflow struct{}

func MvnCleanVerify() Workflow {
	return &mvnCleanVerifyWorkflow{}
}

func (*mvnCleanVerifyWorkflow) Run(args ...string) error {
	return RunForChangedModules(cleanVerifyCmd, args...)
}

func RunForChangedModules(cmd string, args ...string) error {
	flags.RegisterCommonFlags()
	flag.Parse()

	changed := flags.ChangedFiles(javaFileRegex, xmlFileRegex)
	if len(changed) == 0 {
		return nil
	}

	// Collect the modules together for a single call. Maven can work out the install order.
	modules := make([]string, 0)

	// We need to append the base dependency modules, because they are needed to build all
	// other modules.
	modules = append(modules, "metadata")
	modules = append(modules, "it")
	modules = append(modules, "structured-logging")
	modules = append(modules, "plaintext-logging")
	for root, children := range repo.GetModulesForPaths(changed) {
		if len(children) == 0 {
			modules = append(modules, root)
			continue
		}

		// A change to the root POM could impact all children, so build them all.
		buildAll := false
		for _, c := range changed {
			if c == filepath.Join(root, "pom.xml") {
				buildAll = true
				break
			}
		}
		if buildAll {
			modules = append(modules, root)
			continue
		}

		withoutRoot := removeRoot(children)
		if len(withoutRoot) == 0 {
			log.Printf("All files under %s were irrelevant root-level files", root)
		}
		for _, m := range withoutRoot {
			modules = append(modules, fmt.Sprintf("%s/%s", root, m))
		}
	}

	if len(modules) == 0 {
		log.Println("All modules were filtered out.")
		return nil
	}

	return op.RunMavenOnModule(unifiedPom, cmd, strings.Join(modules, ","), args...)
}

type spotlessCheckWorkflow struct{}

func SpotlessCheck() Workflow {
	return &spotlessCheckWorkflow{}
}

func (*spotlessCheckWorkflow) Run(args ...string) error {
	flags.RegisterCommonFlags()
	flag.Parse()

	changed := flags.ChangedFiles(javaFileRegex, markdownFileRegex)
	if len(changed) == 0 {
		return nil
	}

	modules := make([]string, 0)
	for root, children := range repo.GetModulesForPaths(changed) {
		if len(children) == 0 || (len(children) == 1 && children[0] == "") {
			modules = append(modules, root)
			continue
		}
		for _, c := range children {
			modules = append(modules, fmt.Sprintf("%s/%s", root, c))
		}
	}

	return op.RunMavenOnModule(unifiedPom, spotlessCheckCmd, strings.Join(modules, ","), args...)
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
