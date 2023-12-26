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
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	formatJson                      = "json"
	formatTerraform                 = "tf"
	google                          = "google"
	googleBeta                      = "google-beta"
	registryPathGoogle              = "registry.terraform.io/hashicorp/google"
	registryPathGoogleBeta          = "registry.terraform.io/hashicorp/google-beta"
	resourceDataflowJob             = "google_dataflow_job"
	resourceDataflowFlexTemplateJob = "google_dataflow_flex_template_job"
	providerFileName                = "main.tf"
	providerTmplStr                 = `provider "%s" {}`
	versionConstraint               = "~> 1.0"
)

var (
	// format and formatHelp used by preRunE
	format     = formatTerraform
	formatHelp = fmt.Sprintf("Specify the format for output. Allowed: [%s]", strings.Join([]string{
		formatJson,
		formatTerraform,
	}, "|"))

	// formatterMap and output populated by preRunE; used by writeSchema
	formatterMap = map[string]formatter{}
	output       = map[string]io.Writer{
		registryPathGoogle:     os.Stdout,
		registryPathGoogleBeta: os.Stdout,
	}

	// googleProvider, googleBetaProvider, and content used by terraform.stageProvider
	googleProvider     = fmt.Sprintf(providerTmplStr, google)
	googleBetaProvider = fmt.Sprintf(providerTmplStr, googleBeta)
	content            = fmt.Sprintf("%s\n%s\n", googleProvider, googleBetaProvider)
)
