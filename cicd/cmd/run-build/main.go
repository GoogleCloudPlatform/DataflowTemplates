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
	"strings"

	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/flags"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/op"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/repo"
)

const (
	BuildCommand = "clean install"
	FlexPom      = "v2/pom.xml"
)

var (
	args = []string{
		"-am",                 // Include anything this depends on for more reliable builds
		"-amd",                // Include anything that depends on this to guarantee no breaking changes
		"-Dmaven.test.skip",   // Skip tests for verifying this builds
		"-Dmdep.analyze.skip", // TODO(zhoufek): Fix our dependencies then remove this flag
		"-Djib.skip",          // Skip Jib, because don't care about images
	}

	regexes = []string{
		"\\.java$",
		"pom\\.xml$",
	}
)

func main() {
	flags.RegisterCommonFlags()
	flag.Parse()

	changed := flags.ChangedFiles(regexes...)
	if len(changed) == 0 {
		return
	}

	for root, children := range repo.GetModulesForPaths(changed) {
		var err error
		if root == repo.ClassicRoot {
			err = op.RunMavenOnPom(root, BuildCommand, args...)
		} else if root == repo.FlexRoot {
			// A change to the root POM could impact all Flex Templates, so we must rebuild everything on
			// those changes.
			if buildAllFlexTemplates(changed) {
				log.Println("A change indicated to build all Flex Modules. This may take multiple minutes.")
				err = op.RunMavenOnPom(root, BuildCommand, args...)
			} else {
				children = removeRoot(children)
				if len(children) != 0 {
					err = op.RunMavenOnModule(root, BuildCommand, strings.Join(children, ","), args...)
				} else {
					log.Println("The only Flex changes were to files that should not trigger a build")
				}
			}
		}

		if err != nil {
			log.Fatalf("%v", err)
		}
	}
}

// Returns true if all flex templates need to be built. Otherwise, false.
func buildAllFlexTemplates(changed []string) bool {
	for _, c := range changed {
		if c == FlexPom {
			return true
		}
	}
	return false
}

// Removes root and returns results. This may reorder the input.
func removeRoot(flexModules []string) []string {
	var i int
	for i = 0; i < len(flexModules); i += 1 {
		if flexModules[i] == "" {
			break
		}
	}

	if i == len(flexModules) {
		return flexModules
	}

	// Order doesn't matter when passing the modules
	l := len(flexModules)
	flexModules[i] = flexModules[l-1]
	return flexModules[:l-1]
}
