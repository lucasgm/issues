package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultLatestVersions = 1

type Application struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"Description"`
	Active      bool     `json:"active"`
	Teams       []string `json:"teams"`
}

type Environment struct {
	ID            string `json:"id"`
	ApplicationID string `json:"application_id"`
	Application   string `json:"application"`
	Name          string `json:"name"`
	Type          string `json:"type"`
	Active        bool   `json:"active"`
	Prod          bool   `json:"prod"`
	Color         string `json:"color"`
	ResourcePath  string `json:"resource_path"`
}

type Process struct {
	ID            string         `json:"id"`
	Component     string         `json:"component"`
	Name          string         `json:"name"`
	Description   string         `json:"description"`
	Configuration map[string]any `json:"configuration,omitempty"`
}

type Component struct {
	ID            string             `json:"id"`
	Name          string             `json:"name"`
	Application   string             `json:"application"`
	ApplicationID string             `json:"application_id"`
	Description   string             `json:"description"`
	Versions      []ComponentVersion `json:"versions"`
}

type ComponentVersion struct {
	Component string `json:"component"`
	Version   string `json:"version"`
	Created   string `json:"created"`
}

type Snapshot struct {
	ID                string             `json:"id"`
	Application       string             `json:"application"`
	Name              string             `json:"name"`
	Created           string             `json:"created"`
	ComponentVersions []ComponentVersion `json:"component_versions"`
}

type ApplicationInventory struct {
	Application  Application   `json:"application"`
	Environments []Environment `json:"environments"`
	Processes    []Process     `json:"Processes"`
	Components   []Component   `json:"Components"`
	Snapshots    []Snapshot    `json:"Snapshots"`
}

type UCDClient struct {
	baseURL string
	token   string
	http    *http.Client
}

func main() {
	baseURL := flag.String("url", "", "UrbanCode Deploy base URL, example: https://ucd.example.com:8443")
	token := flag.String("token", "", "UrbanCode Deploy auth token")
	insecure := flag.Bool("insecure", false, "Skip TLS certificate verification")
	latestVersions := flag.Int("latest-versions", defaultLatestVersions, "Number of latest component versions to include per component")
	versionDays := flag.Int("version-days", 0, "Only include component versions created within the last N days; 0 disables date filtering")
	flag.Parse()

	if *baseURL == "" || *token == "" {
		fmt.Println("Usage:")
		fmt.Println("  go run ucd.go -url https://ucd.example.com:8443 -token YOUR_TOKEN -insecure")
		os.Exit(1)
	}

	client := NewUCDClient(*baseURL, *token, *insecure)
	apps, err := client.GetApplications()
	if err != nil {
		panic(err)
	}

	results := make([]ApplicationInventory, 0, len(apps))
	for _, app := range apps {
		inventory := ApplicationInventory{
			Application:  app,
			Environments: []Environment{},
			Processes:    []Process{},
			Components:   []Component{},
			Snapshots:    []Snapshot{},
		}

		if envs, err := client.GetApplicationEnvironments(app); err != nil {
			fmt.Fprintf(os.Stderr, "failed to get environments for %s: %v\n", app.Name, err)
		} else {
			inventory.Environments = envs
		}

		if processes, err := client.GetApplicationProcesses(app); err != nil {
			fmt.Fprintf(os.Stderr, "failed to get processes for %s: %v\n", app.Name, err)
		} else {
			inventory.Processes = processes
		}

		if components, err := client.GetApplicationComponents(app, *latestVersions, *versionDays); err != nil {
			fmt.Fprintf(os.Stderr, "failed to get components for %s: %v\n", app.Name, err)
		} else {
			inventory.Components = components
		}

		if componentProcesses, err := client.GetComponentProcesses(inventory.Components); err != nil {
			fmt.Fprintf(os.Stderr, "failed to get component processes for %s: %v\n", app.Name, err)
		} else {
			inventory.Processes = append(inventory.Processes, componentProcesses...)
		}

		if snapshots, err := client.GetApplicationSnapshots(app); err != nil {
			fmt.Fprintf(os.Stderr, "failed to get snapshots for %s: %v\n", app.Name, err)
		} else {
			inventory.Snapshots = snapshots
		}

		results = append(results, inventory)
	}

	out, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(out))
}

func NewUCDClient(baseURL, token string, insecure bool) *UCDClient {
	return &UCDClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		http: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
			},
		},
	}
}

func (c *UCDClient) GetApplications() ([]Application, error) {
	var rawApps []map[string]any
	if err := c.getJSON("cli/application", nil, &rawApps); err != nil {
		return nil, err
	}

	apps := make([]Application, 0, len(rawApps))
	for _, rawApp := range rawApps {
		appID := pickString(rawApp, "id", "uuid")
		teams := c.getAppTeams(appID)
		apps = append(apps, Application{
			ID:          pickString(rawApp, "id", "uuid"),
			Name:        pickString(rawApp, "name"),
			Description: pickString(rawApp, "description", "Description"),
			Active:      pickBoolDefault(rawApp, true, "active"),
			Teams:       teams,
		})
	}

	return apps, nil
}

func (c *UCDClient) getAppTeams(appID string) []string {
	if appID == "" {
		return []string{}
	}

	var raw map[string]any
	if err := c.getJSONOptional("rest/deploy/application/"+appID, nil, &raw); err != nil || raw == nil {
		return []string{}
	}

	extSec, ok := raw["extendedSecurity"].(map[string]any)
	if !ok {
		return []string{}
	}

	teamsRaw, ok := extSec["teams"].([]any)
	if !ok {
		return []string{}
	}

	teams := make([]string, 0, len(teamsRaw))
	for _, entry := range teamsRaw {
		entryMap, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		team, ok := entryMap["team"].(map[string]any)
		if !ok {
			continue
		}
		if name := pickString(team, "name"); name != "" {
			teams = append(teams, name)
		}
	}
	return teams
}

func (c *UCDClient) GetApplicationEnvironments(app Application) ([]Environment, error) {
	envs, err := c.getApplicationEnvironmentsByFilter("application.name", app.Name)
	if err != nil || len(envs) > 0 {
		return envs, err
	}

	envs, err = c.getApplicationEnvironmentsByFilter("application.id", app.ID)
	if err != nil || len(envs) > 0 {
		return envs, err
	}

	return c.getApplicationEnvironmentsByCLI(app)
}

func (c *UCDClient) getApplicationEnvironmentsByFilter(field, value string) ([]Environment, error) {
	query := url.Values{}
	query.Set("filterFields", field)
	query.Set("filterType_"+field, "eq")
	query.Set("filterValue_"+field, value)
	query.Set("rowsPerPage", "1000")

	var rawEnvs []map[string]any
	if err := c.getJSON("rest/deploy/environment", query, &rawEnvs); err != nil {
		return nil, err
	}

	return normalizeEnvironments(rawEnvs), nil
}

func (c *UCDClient) getApplicationEnvironmentsByCLI(app Application) ([]Environment, error) {
	query := url.Values{}
	query.Set("application", app.Name)

	var rawEnvs []map[string]any
	if err := c.getJSON("cli/application/environmentsInApplication", query, &rawEnvs); err != nil {
		return nil, err
	}

	return normalizeEnvironments(rawEnvs), nil
}

func (c *UCDClient) GetApplicationProcesses(app Application) ([]Process, error) {
	paths := []string{
		"cli/applicationProcess",
		"cli/applicationProcess/processesInApplication",
	}
	applications := []string{app.Name, app.ID}
	seen := map[string]bool{}
	processes := []Process{}

	for _, path := range paths {
		for _, application := range applications {
			if application == "" {
				continue
			}

			query := url.Values{}
			query.Set("application", application)

			var rawProcesses []map[string]any
			if err := c.getJSONOptional(path, query, &rawProcesses); err != nil {
				return processes, err
			}

			for _, rawProcess := range rawProcesses {
				process := normalizeProcess(rawProcess, "")
				key := firstNonEmpty(process.ID, process.Name)
				if key == "" || seen[key] {
					continue
				}
				seen[key] = true
				processes = append(processes, process)
			}

			if len(processes) > 0 {
				return processes, nil
			}
		}
	}

	return processes, nil
}

func (c *UCDClient) GetApplicationComponents(app Application, latestVersions, versionDays int) ([]Component, error) {
	query := url.Values{}
	query.Set("application", app.Name)

	var rawComponents []map[string]any
	if err := c.getJSONOptional("cli/application/componentsInApplication", query, &rawComponents); err != nil {
		return nil, err
	}

	components := make([]Component, 0, len(rawComponents))
	for _, rawComponent := range rawComponents {
		component := normalizeComponent(rawComponent, app)
		versions, err := c.GetComponentVersions(component, latestVersions, versionDays)
		if err != nil {
			return components, err
		}
		component.Versions = versions
		components = append(components, component)
	}

	return components, nil
}

func (c *UCDClient) GetComponentVersions(component Component, latestVersions, versionDays int) ([]ComponentVersion, error) {
	if latestVersions < 0 {
		latestVersions = 0
	}

	query := url.Values{}
	query.Set("component", component.Name)

	var rawVersions []map[string]any
	if err := c.getJSONOptional("cli/version", query, &rawVersions); err != nil {
		return nil, err
	}

	versions := make([]ComponentVersion, 0, len(rawVersions))
	for _, rawVersion := range rawVersions {
		version := normalizeComponentVersion(rawVersion, component.Name)
		if version.Version == "" {
			continue
		}
		if versionDays > 0 && !createdWithinDays(version.Created, versionDays) {
			continue
		}
		versions = append(versions, version)
		if latestVersions > 0 && len(versions) >= latestVersions {
			break
		}
	}

	return versions, nil
}

func (c *UCDClient) GetComponentProcesses(components []Component) ([]Process, error) {
	processes := []Process{}
	for _, component := range components {
		query := url.Values{}
		query.Set("component", component.Name)

		var rawProcesses []map[string]any
		if err := c.getJSONOptional("cli/componentProcess", query, &rawProcesses); err != nil {
			return processes, err
		}

		for _, rawProcess := range rawProcesses {
			processes = append(processes, normalizeProcess(rawProcess, component.Name))
		}
	}

	return processes, nil
}

func (c *UCDClient) GetApplicationSnapshots(app Application) ([]Snapshot, error) {
	query := url.Values{}
	query.Set("application", app.Name)

	var rawSnapshots []map[string]any
	if err := c.getJSONOptional("cli/snapshot", query, &rawSnapshots); err != nil {
		return nil, err
	}

	snapshots := make([]Snapshot, 0, len(rawSnapshots))
	for _, rawSnapshot := range rawSnapshots {
		snapshots = append(snapshots, normalizeSnapshot(rawSnapshot, app.Name))
	}

	return snapshots, nil
}

func (c *UCDClient) getJSON(path string, query url.Values, target any) error {
	requestURL := c.baseURL + "/" + strings.TrimLeft(path, "/")
	if len(query) > 0 {
		requestURL += "?" + query.Encode()
	}

	req, err := http.NewRequest(http.MethodGet, requestURL, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth("PasswordIsAuthToken", c.token)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GET %s failed: HTTP %d: %s", requestURL, resp.StatusCode, string(body))
	}

	if len(strings.TrimSpace(string(body))) == 0 {
		return nil
	}

	return json.Unmarshal(body, target)
}

func (c *UCDClient) getJSONOptional(path string, query url.Values, target any) error {
	err := c.getJSON(path, query, target)
	if err == nil {
		return nil
	}

	errText := err.Error()
	if strings.Contains(errText, "HTTP 404") || strings.Contains(errText, "HTTP 405") {
		return nil
	}

	return err
}

func normalizeEnvironments(rawEnvs []map[string]any) []Environment {
	envs := make([]Environment, 0, len(rawEnvs))
	for _, rawEnv := range rawEnvs {
		appID, appName := nestedApplication(rawEnv)
		envs = append(envs, Environment{
			ID:            pickString(rawEnv, "id", "uuid"),
			ApplicationID: firstNonEmpty(pickString(rawEnv, "applicationId", "application_id"), appID),
			Application:   firstNonEmpty(pickString(rawEnv, "application", "applicationName"), appName),
			Name:          pickString(rawEnv, "name"),
			Type:          pickString(rawEnv, "type", "environmentType"),
			Active:        pickBoolDefault(rawEnv, true, "active"),
			Prod:          pickBoolDefault(rawEnv, false, "prod", "production", "isProd"),
			Color:         pickString(rawEnv, "color", "colorCode"),
			ResourcePath:  pickString(rawEnv, "resourcePath", "resource_path", "baseResourcePath"),
		})
	}

	return envs
}

func normalizeProcess(rawProcess map[string]any, componentName string) Process {
	return Process{
		ID:            pickString(rawProcess, "id", "uuid"),
		Component:     firstNonEmpty(pickString(rawProcess, "component", "componentName"), componentName),
		Name:          pickString(rawProcess, "name"),
		Description:   pickString(rawProcess, "description"),
		Configuration: pickProcessConfiguration(rawProcess),
	}
}

func normalizeComponent(rawComponent map[string]any, app Application) Component {
	return Component{
		ID:            pickString(rawComponent, "id", "uuid"),
		Name:          pickString(rawComponent, "name"),
		Application:   app.Name,
		ApplicationID: app.ID,
		Description:   pickString(rawComponent, "description"),
	}
}

func normalizeSnapshot(rawSnapshot map[string]any, appName string) Snapshot {
	return Snapshot{
		ID:                pickString(rawSnapshot, "id", "uuid"),
		Application:       firstNonEmpty(pickString(rawSnapshot, "application", "applicationName"), appName),
		Name:              pickString(rawSnapshot, "name"),
		Created:           pickString(rawSnapshot, "created", "createdDate", "createdOn"),
		ComponentVersions: pickComponentVersions(rawSnapshot),
	}
}

func normalizeComponentVersion(rawVersion map[string]any, componentName string) ComponentVersion {
	return ComponentVersion{
		Component: firstNonEmpty(pickString(rawVersion, "component", "componentName"), componentName),
		Version:   pickString(rawVersion, "version", "versionName", "name"),
		Created:   pickString(rawVersion, "created", "createdDate", "createdOn"),
	}
}

func nestedApplication(values map[string]any) (string, string) {
	app, ok := values["application"].(map[string]any)
	if !ok {
		return "", ""
	}
	return pickString(app, "id", "uuid"), pickString(app, "name")
}

func pickString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := values[key]; ok {
			switch typed := value.(type) {
			case string:
				return typed
			case float64:
				return strconv.FormatFloat(typed, 'f', -1, 64)
			case bool:
				return strconv.FormatBool(typed)
			}
		}
	}

	return ""
}

func pickBoolDefault(values map[string]any, defaultValue bool, keys ...string) bool {
	for _, key := range keys {
		value, ok := values[key]
		if !ok {
			continue
		}

		switch typed := value.(type) {
		case bool:
			return typed
		case string:
			parsed, err := strconv.ParseBool(typed)
			if err == nil {
				return parsed
			}
		}
	}

	return defaultValue
}

func pickStringSlice(values map[string]any, keys ...string) []string {
	for _, key := range keys {
		value, ok := values[key]
		if !ok {
			continue
		}

		switch typed := value.(type) {
		case []string:
			return typed
		case []any:
			items := make([]string, 0, len(typed))
			for _, item := range typed {
				switch itemValue := item.(type) {
				case string:
					items = append(items, itemValue)
				case map[string]any:
					name := pickString(itemValue, "name", "teamName", "id")
					if name != "" {
						items = append(items, name)
					}
				}
			}
			return items
		case string:
			if typed == "" {
				return []string{}
			}
			return []string{typed}
		}
	}

	return []string{}
}

func pickProcessConfiguration(values map[string]any) map[string]any {
	configuration := pickMap(values, "configuration", "Configuration")
	if configuration == nil {
		configuration = map[string]any{}
	}

	if basicSettings := pickMap(values, "basicSettings", "basic_settings", "BasicSettings"); basicSettings != nil {
		configuration["basic_settings"] = basicSettings
	}

	if len(configuration) == 0 {
		return nil
	}

	return configuration
}

func pickMap(values map[string]any, keys ...string) map[string]any {
	for _, key := range keys {
		value, ok := values[key]
		if !ok {
			continue
		}

		typed, ok := value.(map[string]any)
		if ok {
			return typed
		}
	}

	return nil
}

func pickComponentVersions(values map[string]any) []ComponentVersion {
	for _, key := range []string{"componentVersions", "component_versions", "versions"} {
		value, ok := values[key]
		if !ok {
			continue
		}

		items, ok := value.([]any)
		if !ok {
			continue
		}

		versions := make([]ComponentVersion, 0, len(items))
		for _, item := range items {
			itemMap, ok := item.(map[string]any)
			if !ok {
				continue
			}

			versions = append(versions, ComponentVersion{
				Component: pickNestedString(itemMap, []string{"component", "name"}, "component", "componentName", "name"),
				Version:   pickNestedString(itemMap, []string{"version", "name"}, "version", "versionName", "name"),
				Created:   pickString(itemMap, "created", "createdDate", "createdOn"),
			})
		}

		return versions
	}

	return []ComponentVersion{}
}

func pickNestedString(values map[string]any, nestedPath []string, fallbackKeys ...string) string {
	current := any(values)
	for _, key := range nestedPath {
		currentMap, ok := current.(map[string]any)
		if !ok {
			return pickString(values, fallbackKeys...)
		}
		current = currentMap[key]
	}

	if value, ok := current.(string); ok {
		return value
	}

	return pickString(values, fallbackKeys...)
}

func createdWithinDays(created string, days int) bool {
	if created == "" {
		return true
	}

	createdTime, ok := parseUCDTime(created)
	if !ok {
		return true
	}

	return createdTime.After(time.Now().AddDate(0, 0, -days))
}

func parseUCDTime(value string) (time.Time, bool) {
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.000-0700",
		"2006-01-02T15:04:05-0700",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed, true
		}
	}

	return time.Time{}, false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}

	return ""
}
