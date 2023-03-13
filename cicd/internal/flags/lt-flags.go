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

package flags

import (
	"flag"
)

// Avoid making these vars public.
var (
	dexportProject string
	dexportDataset string
	dexportTable   string
)

// Registers all common flags. Must be called before flag.Parse().
func RegisterLtFlags() {
	flag.StringVar(&dexportProject, "lt-export-project", "", "The GCP project to export load test metrics")
	flag.StringVar(&dexportDataset, "lt-export-dataset", "", "The GCP BigQuery dataset to export metrics")
	flag.StringVar(&dexportTable, "lt-export-table", "", "A GCP BigQuery table to store metrics")
}

func ExportProject() string {
	return "-DexportProject=" + dexportProject
}

func ExportDataset() string {
	return "-DexportDataset=" + dexportDataset
}

func ExportTable() string {
	return "-DexportTable=" + dexportTable
}
