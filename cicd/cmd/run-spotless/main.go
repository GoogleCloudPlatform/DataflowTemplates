/*
 * Copyright (C) 2021 Google LLC
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
	"fmt"
	"log"
	"strings"

	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/op"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/repo"
)

const (
	SpotlessCommand = "spotless:check"
)

func main() {
	changed := flag.String("changed-files", "", "List of changed files as a comma-separated string")
	flag.Parse()

	if len(*changed) == 0 {
		log.Print("No changed files passed. This is probably an error, but we're assuming it isn't just in case")
		return
	}
	log.Printf("Received changed files: %s", *changed)

	s := strings.Split(*changed, ",")
	modules := repo.GetModulesForPaths(s)

	var fullErr error
	if _, ok := modules[repo.ClassicRoot]; ok {
		if err := op.RunMavenOnPom(repo.ClassicRoot, SpotlessCommand); err != nil {
			fullErr = err
		}
	}
	if flex, ok := modules[repo.FlexRoot]; ok {
		for _, m := range flex {
			if len(m) > 0 { // Zero length represents the root, which has no Java code
				if err := op.RunMavenOnModule(repo.FlexRoot, SpotlessCommand, m); err != nil && fullErr != nil {
					fullErr = err
				} else if err != nil {
					fullErr = fmt.Errorf("%w\n%v", fullErr, err)
				}
			}
		}
	}

	if fullErr != nil {
		log.Fatal("There were spotless errors. Check the output from the commands.")
	}
}
