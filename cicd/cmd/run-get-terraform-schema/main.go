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

// The run-get-terraform-schema workflow acquires the schema of Dataflow related resources of the Google and
// Google Beta providers.
package main

import (
	install "github.com/hashicorp/hc-install"
	"github.com/hashicorp/terraform-exec/tfexec"
)

func main() {
	if err := command.Execute(); err != nil {
		panic(err)
	}
}

// terraform executes the workflow of installing the command-line executable, if needed, staging provider configuration,
// and getting the Google and Google-Beta resource schemas.
type terraform struct {
	execPath   string
	installer  *install.Installer
	workingDir string
	targetDir  string
	cli        *tfexec.Terraform
}
