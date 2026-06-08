package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type Application struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Environment struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type AppEnvironments struct {
	Application  Application   `json:"application"`
	Environments []Environment `json:"environments"`
}

func main() {
	baseURL := flag.String("url", "", "UrbanCode Deploy base URL, example: https://ucd.example.com:8443")
	token := flag.String("token", "", "UrbanCode Deploy auth token")
	insecure := flag.Bool("insecure", false, "Skip TLS certificate verification")
	flag.Parse()

	if *baseURL == "" || *token == "" {
		fmt.Println("Usage:")
		fmt.Println("  go run main.go -url https://ucd.example.com:8443 -token YOUR_TOKEN -insecure")
		os.Exit(1)
	}

	client := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: *insecure},
		},
	}

	apps, err := getApplications(client, *baseURL, *token)
	if err != nil {
		panic(err)
	}

	var results []AppEnvironments

	for _, app := range apps {
		envs, err := getApplicationEnvironments(client, *baseURL, *token, app.ID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get environments for %s: %v\n", app.Name, err)
			continue
		}

		results = append(results, AppEnvironments{
			Application:  app,
			Environments: envs,
		})
	}

	out, _ := json.MarshalIndent(results, "", "  ")
	fmt.Println(string(out))
}

func getApplications(client *http.Client, baseURL, token string) ([]Application, error) {
	url := strings.TrimRight(baseURL, "/") + "/cli/application"

	var apps []Application
	err := doGet(client, url, token, &apps)
	return apps, err
}

func getApplicationEnvironments(client *http.Client, baseURL, token, appID string) ([]Environment, error) {
	url := strings.TrimRight(baseURL, "/") + "/cli/application/environmentsInApplication?application=" + appID

	var envs []Environment
	err := doGet(client, url, token, &envs)
	return envs, err
}

func doGet(client *http.Client, url, token string, target any) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Accept", "application/json")

	// UrbanCode/DevOps Deploy tokens are commonly passed as username with empty password.
	req.SetBasicAuth(token, "")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GET %s failed: HTTP %d: %s", url, resp.StatusCode, string(body))
	}

	return json.Unmarshal(body, target)
}

//output
[
  {
    "application": {
      "id": "abc-123",
      "name": "my-app"
    },
    "environments": [
      {
        "id": "env-123",
        "name": "DEV"
      }
    ]
  }
]
