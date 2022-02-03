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

	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/flags"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/op"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/repo"
)

const (
	SpotlessCommand = "spotless:check"
)

func main() {
	flags.RegisterCommonFlags()
	flag.Parse()

	var modules map[string][]string
	if changed := flags.ChangedFiles(); len(changed) == 0 {
		return
	} else {
		modules = repo.GetModulesForPaths(changed)
	}

	var fullErr error
	for _, root := range repo.GetAllRoots() {
		if children, ok := modules[root]; ok {
			var err error
			if len(children) == 0 {
				err = op.RunMavenOnPom(root, SpotlessCommand)
			} else {
				err = op.RunMavenOnModule(root, SpotlessCommand, strings.Join(children, ","))
			}

			if err != nil && fullErr == nil {
				fullErr = err
			} else if err != nil {
				fullErr = fmt.Errorf("%w\n%v", fullErr, err)
			}
		}
	}

	if fullErr != nil {
		log.Fatal("There were spotless errors. Check the output from the commands.")
	}
}
