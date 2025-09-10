package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

/* ---------------------- Input model ---------------------- */

type ExecEntry struct {
	ID          string      `json:"id"`
	Action      string      `json:"action"`      // e.g. DOWNLOAD/INSTALL/START/STOP/...
	ServerInfo  string      `json:"serverInfo"`  // actual host
	Description string      `json:"description"` // may contain :<version>
	Component   *Component  `json:"component,omitempty"`
	AllComps    []Component `json:"allComponents,omitempty"`
}

type Component struct {
	Name          string      `json:"name"`
	Type          string      `json:"type"`
	ActionCmds    []ActionCmd `json:"actionCommands"`
	ServerFilters []string    `json:"serverFilters"`
	DependsOn     []CompDep   `json:"dependsOn"`
	Containers    []Container `json:"containers"`
}

type ActionCmd struct {
	Name                       string   `json:"name"`
	Command                    string   `json:"command"`
	Action                     string   `json:"action,omitempty"`
	DependsOn                  []string `json:"dependsOn"`
	RunStage                   string   `json:"runStage,omitempty"`
	CascadeCommandDependencies *bool    `json:"cascadeCommandDependencies,omitempty"`
}

type CompDep struct {
	ComponentName     string  `json:"componentName"`
	Action            *string `json:"action,omitempty"`          // current component action context
	DependsOnAction   *string `json:"dependsOnAction,omitempty"` // dependency action requirement
	ServerFilter      string  `json:"serverFilter,omitempty"`
	ComponentFilter   string  `json:"componentFilter,omitempty"`
	ReverseDependency *bool   `json:"reverseDependency,omitempty"`
}

type Container struct {
	Type           string          `json:"type"`
	GroupID        string          `json:"groupId"`
	ArtifactID     string          `json:"artifactId"`
	RPMName        string          `json:"rpmName"`
	ServiceCommand *ServiceCommand `json:"serviceCommands,omitempty"`
	// ZIP-specific / distribution fields
	InstallLocation  string `json:"installLocation,omitempty"`
	PostUnZipCmd     string `json:"postUnZipCmd,omitempty"`
	InstallCommand   string `json:"installCommand,omitempty"`
	UninstallCommand string `json:"uninstallCommand,omitempty"`
	ContainerType    string `json:"containerType,omitempty"`
}

type ServiceCommand struct {
	Stop       string `json:"stop"`
	Start      string `json:"start"`
	Health     string `json:"health"`
	StopHealth string `json:"stopHealth"`
}

/* ---------------------- Output model ---------------------- */

type OutService struct {
	Name               string         `yaml:"name"`
	Type               string         `yaml:"type"`
	DependsOn          []DependsOnOut `yaml:"dependsOn"`
	ActionCmds         []OutActionCmd `yaml:"actionCommands"`
	Containers         []OutContainer `yaml:"containers"`
	ServerHosts        []string       `yaml:"serverFilters"` // actual hosts for this service
	Lifecycle          []string       `yaml:"lifecycle,omitempty"`
	TopologicalParents []string       `yaml:"-"`
}

type DependsOnOut struct {
	ComponentName     string `yaml:"componentName"`
	ServerFilter      string `yaml:"serverFilter,omitempty"`
	ReverseDependency bool   `yaml:"reverseDependency"`

	// carry orchestration semantics directly in dependsOn
	CurrentAction     string `yaml:"currentAction,omitempty"`
	DependsOnAction   string `yaml:"dependsOnAction,omitempty"`
	ComponentFilter   string `yaml:"componentFilter,omitempty"`
	OrchestrationOnly bool   `yaml:"orchestrationOnly,omitempty"`
}

type OutActionCmd struct {
	Name      string   `yaml:"name"`
	Command   string   `yaml:"command"`
	Action    string   `yaml:"action,omitempty"`
	RunStage  string   `yaml:"runStage,omitempty"`
	DependsOn []string `yaml:"dependsOn,omitempty"`
}

type OutContainer struct {
	Type           string            `yaml:"type"`
	GroupID        string            `yaml:"groupId"`
	ArtifactID     string            `yaml:"artifactId"`
	Version        string            `yaml:"version"`
	RPMName        string            `yaml:"rpmName"`
	ServiceCommand map[string]string `yaml:"serviceCommands,omitempty"`
	// ZIP/distribution metadata (optional)
	InstallLocation  string `yaml:"installLocation,omitempty"`
	PostUnZipCmd     string `yaml:"postUnZipCmd,omitempty"`
	InstallCommand   string `yaml:"installCommand,omitempty"`
	UninstallCommand string `yaml:"uninstallCommand,omitempty"`
	ContainerType    string `yaml:"containerType,omitempty"`
}

/* ---------------------- Helpers ---------------------- */

var versionRe = regexp.MustCompile(`:([0-9]+(?:\.[0-9]+){0,3})\b`)

func uniqStrings(in []string) []string {
	m := map[string]struct{}{}
	for _, s := range in {
		if strings.TrimSpace(s) == "" {
			continue
		}
		m[s] = struct{}{}
	}
	out := make([]string, 0, len(m))
	for s := range m {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// prefer the most recent specific hint: last INSTALL version, else last DOWNLOAD version
func pickVersion(installVers []string, downloadVers []string) string {
	if n := len(installVers); n > 0 {
		return installVers[n-1]
	}
	if n := len(downloadVers); n > 0 {
		return downloadVers[n-1]
	}
	return ""
}

func isSameHostFilter(s string) bool {
	// best-effort detect the common same-host gate
	return strings.Contains(s, "ice.host.name=${ice.host.name}")
}

// treat only these as deploy-time actions; do NOT learn from UNINSTALL/CREATE/etc
func isDeployAction(act string) bool {
	switch strings.ToUpper(strings.TrimSpace(act)) {
	case "DOWNLOAD", "INSTALL", "START", "STOP", "HEALTH", "HEALTH_CHECK":
		return true
	default:
		return false
	}
}

/* ---------------------- DAG for topo order ---------------------- */

type dag struct {
	nodes map[string]struct{}
	adj   map[string]map[string]struct{} // u -> v
	indeg map[string]int
}

func newDAG() *dag {
	return &dag{
		nodes: map[string]struct{}{},
		adj:   map[string]map[string]struct{}{},
		indeg: map[string]int{},
	}
}
func (g *dag) addNode(n string) {
	if n == "" {
		return
	}
	if _, ok := g.nodes[n]; ok {
		return
	}
	g.nodes[n] = struct{}{}
	g.adj[n] = map[string]struct{}{}
	g.indeg[n] = 0
}
func (g *dag) addEdge(u, v string) {
	if u == "" || v == "" || u == v {
		return
	}
	g.addNode(u)
	g.addNode(v)
	if _, ok := g.adj[u][v]; ok {
		return
	}
	g.adj[u][v] = struct{}{}
	g.indeg[v]++
}
func (g *dag) topo() ([]string, error) {
	var q []string
	for n := range g.nodes {
		if g.indeg[n] == 0 {
			q = append(q, n)
		}
	}
	sort.Strings(q)
	var order []string
	for len(q) > 0 {
		u := q[0]
		q = q[1:]
		order = append(order, u)
		for v := range g.adj[u] {
			g.indeg[v]--
			if g.indeg[v] == 0 {
				q = append(q, v)
			}
		}
		sort.Strings(q)
	}
	if len(order) != len(g.nodes) {
		return nil, fmt.Errorf("cycle detected in dependencies")
	}
	return order, nil
}

/* ---------------------- Aggregation model (package-scope) ---------------------- */

type agg struct {
	compType     string
	actionCmds   []ActionCmd
	containers   []Container
	deps         []CompDep
	hostSet      map[string]struct{}
	versInstall  []string
	versDownload []string
}

/* ---------------------- Main ---------------------- */

func main() {
	in := flag.String("in", "", "Path to exec plan JSON")
	out := flag.String("out", "execplan.yml", "Path to YAML output")
	flag.Parse()

	if strings.TrimSpace(*in) == "" {
		fmt.Fprintln(os.Stderr, "ERROR: -in <exec_plan.json> is required")
		os.Exit(1)
	}

	raw, err := os.ReadFile(*in)
	if err != nil {
		fmt.Fprintln(os.Stderr, "read:", err)
		os.Exit(1)
	}

	var entries []ExecEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		fmt.Fprintln(os.Stderr, "json:", err)
		os.Exit(1)
	}

	// Track actions and hosts (only from deploy-time actions), using ONLY the primary .component
	hasAction := map[string]map[string]struct{}{}                  // comp -> set(actions)
	compHosts := map[string]map[string]struct{}{}                  // comp -> set(hosts)
	hasActionByHost := map[string]map[string]map[string]struct{}{} // comp -> host -> set(actions)

	for _, e := range entries {
		act := strings.ToUpper(strings.TrimSpace(e.Action))
		if !isDeployAction(act) {
			continue // do not learn anything from UNINSTALL/CREATE/etc
		}
		if e.Component == nil {
			continue
		}
		c := e.Component
		name := strings.TrimSpace(c.Name)
		if name == "" {
			continue
		}

		// init maps
		if _, ok := hasAction[name]; !ok {
			hasAction[name] = map[string]struct{}{}
		}
		if _, ok := compHosts[name]; !ok {
			compHosts[name] = map[string]struct{}{}
		}
		if _, ok := hasActionByHost[name]; !ok {
			hasActionByHost[name] = map[string]map[string]struct{}{}
		}

		// action presence (deploy-time only)
		hasAction[name][act] = struct{}{}

		// host binding (deploy-time only)
		if e.ServerInfo != "" {
			compHosts[name][e.ServerInfo] = struct{}{}
			if _, ok := hasActionByHost[name][e.ServerInfo]; !ok {
				hasActionByHost[name][e.ServerInfo] = map[string]struct{}{}
			}
			hasActionByHost[name][e.ServerInfo][act] = struct{}{}
		}
	}

	// Aggregate per component
	comps := map[string]*agg{}
	get := func(name string) *agg {
		a := comps[name]
		if a == nil {
			a = &agg{hostSet: map[string]struct{}{}}
			comps[name] = a
		}
		return a
	}

	for _, e := range entries {
		act := strings.ToUpper(strings.TrimSpace(e.Action))
		// Only aggregate from deploy-time actions to avoid promoting uninstall listings
		if !isDeployAction(act) {
			continue
		}
		if e.Component == nil {
			continue
		}

		c := e.Component
		name := strings.TrimSpace(c.Name)
		if name == "" {
			continue
		}
		a := get(name)

		// type/commands/containers/deps are learned only from deploy-time actions
		if a.compType == "" {
			a.compType = c.Type
		}
		if len(a.actionCmds) == 0 && len(c.ActionCmds) > 0 {
			a.actionCmds = append(a.actionCmds, c.ActionCmds...)
		}
		if len(a.containers) == 0 && len(c.Containers) > 0 {
			a.containers = append(a.containers, c.Containers...)
		}
		if len(c.DependsOn) > 0 {
			a.deps = append(a.deps, c.DependsOn...)
			for _, d := range c.DependsOn {
				if d.ComponentName != "" {
					get(d.ComponentName) // ensure node exists
				}
			}
		}
		// host binding from deploy-time actions only
		if hset, ok := compHosts[name]; ok {
			for h := range hset {
				a.hostSet[h] = struct{}{}
			}
		}

		// version hints from description on INSTALL/DOWNLOAD (deploy-time only)
		if (act == "INSTALL" || act == "DOWNLOAD") && strings.TrimSpace(e.Description) != "" {
			if m := versionRe.FindStringSubmatch(e.Description); len(m) == 2 {
				v := m[1]
				if act == "INSTALL" {
					a.versInstall = append(a.versInstall, v) // keep order; prefer last
				} else {
					a.versDownload = append(a.versDownload, v) // keep order; prefer last
				}
			}
		}
	}

	// Build topo graph from deps (edge direction respects reverseDependency)
	g := newDAG()
	for name := range comps {
		g.addNode(name)
	}
	for to, a := range comps {
		for _, d := range a.deps {
			from := d.ComponentName
			rev := d.ReverseDependency != nil && *d.ReverseDependency
			u, v := from, to
			if rev {
				u, v = to, from
			}
			g.addEdge(u, v)
		}
	}

	// Order (use alpha fallback if cycle)
	order, err := g.topo()
	if err != nil {
		var names []string
		for k := range comps {
			names = append(names, k)
		}
		sort.Strings(names)
		order = names
	}

	// Emit YAML list
	var outList []OutService
	for _, name := range order {
		a := comps[name]
		if a == nil {
			continue
		}

		// Hosts
		var hosts []string
		for h := range a.hostSet {
			hosts = append(hosts, h)
		}
		sort.Strings(hosts)

		// Version
		version := pickVersion(a.versInstall, a.versDownload)

		// Containers
		var outs []OutContainer
		for _, c := range a.containers {
			oc := OutContainer{
				Type:       c.Type,
				GroupID:    c.GroupID,
				ArtifactID: c.ArtifactID,
				Version:    version,
				RPMName:    c.RPMName,
			}
			if c.ServiceCommand != nil {
				oc.ServiceCommand = map[string]string{}
				if c.ServiceCommand.Stop != "" {
					oc.ServiceCommand["stop"] = c.ServiceCommand.Stop
				}
				if c.ServiceCommand.Start != "" {
					oc.ServiceCommand["start"] = c.ServiceCommand.Start
				}
				if c.ServiceCommand.Health != "" {
					oc.ServiceCommand["health"] = c.ServiceCommand.Health
				}
				if c.ServiceCommand.StopHealth != "" {
					oc.ServiceCommand["stopHealth"] = c.ServiceCommand.StopHealth
				}
				if len(oc.ServiceCommand) == 0 {
					oc.ServiceCommand = nil
				}
			}

			// ZIP/distribution specific fields
			if strings.TrimSpace(c.InstallLocation) != "" {
				oc.InstallLocation = c.InstallLocation
			}
			if strings.TrimSpace(c.PostUnZipCmd) != "" {
				oc.PostUnZipCmd = c.PostUnZipCmd
			}
			if strings.TrimSpace(c.InstallCommand) != "" {
				oc.InstallCommand = c.InstallCommand
			}
			if strings.TrimSpace(c.UninstallCommand) != "" {
				oc.UninstallCommand = c.UninstallCommand
			}
			if strings.TrimSpace(c.ContainerType) != "" {
				oc.ContainerType = c.ContainerType
			}
			outs = append(outs, oc)
		}

		// Dependencies (carry orchestration semantics directly)
		depObjs := buildDependsObjects(name, a.deps, hosts, hasAction, hasActionByHost)

		// Lifecycle (emit only actions actually present; no auto symmetry)
		lifecycle := computeLifecycle(name, a, hasAction)

		outList = append(outList, OutService{
			Name:        name,
			Type:        a.compType,
			DependsOn:   depObjs,
			ActionCmds:  mapActionCmds(a.actionCmds),
			Containers:  outs,
			ServerHosts: hosts, // actual host list
			Lifecycle:   lifecycle,
		})
	}

	// Write YAML (single document: list of components)
	y, err := yaml.Marshal(outList)
	if err != nil {
		fmt.Fprintln(os.Stderr, "yaml:", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, y, 0644); err != nil {
		fmt.Fprintln(os.Stderr, "write:", err)
		os.Exit(1)
	}
}

func mapActionCmds(in []ActionCmd) []OutActionCmd {
	var out []OutActionCmd
	for _, ac := range in {
		out = append(out, OutActionCmd{
			Name:      ac.Name,
			Command:   ac.Command,
			Action:    ac.Action,
			RunStage:  ac.RunStage,
			DependsOn: uniqStrings(ac.DependsOn),
		})
	}
	return out
}

// buildDependsObjects merges duplicates but keeps orchestration metadata and marks orchestrationOnly.
// For same-host gates we check actions on the same hosts where the parent component runs.
func buildDependsObjects(
	parentName string,
	deps []CompDep,
	parentHosts []string,
	hasAction map[string]map[string]struct{},
	hasActionByHost map[string]map[string]map[string]struct{},
) []DependsOnOut {

	type key struct {
		name              string
		filter            string
		rev               bool
		curAct            string
		depAct            string
		compFilter        string
		orchestrationOnly bool
	}

	seen := map[key]struct{}{}
	var out []DependsOnOut

	for _, d := range deps {
		if strings.TrimSpace(d.ComponentName) == "" {
			continue
		}
		curAct := ""
		if d.Action != nil {
			curAct = strings.ToUpper(strings.TrimSpace(*d.Action))
		}
		depAct := ""
		if d.DependsOnAction != nil {
			depAct = strings.ToUpper(strings.TrimSpace(*d.DependsOnAction))
		}
		filter := strings.TrimSpace(d.ServerFilter)
		compFilter := strings.TrimSpace(d.ComponentFilter)
		rev := d.ReverseDependency != nil && *d.ReverseDependency

		// Decide orchestrationOnly (host-aware for same-host filter)
		depOnly := true
		if depAct != "" {
			if isSameHostFilter(filter) && len(parentHosts) > 0 {
				// same-host: require the action on at least one of the parent hosts
				if hostMap, ok := hasActionByHost[d.ComponentName]; ok {
					for _, h := range parentHosts {
						if acts, ok2 := hostMap[h]; ok2 {
							if _, ok3 := acts[depAct]; ok3 {
								depOnly = false
								break
							}
						}
					}
				}
			} else {
				// global fallback: action anywhere in the plan
				if acts, ok := hasAction[d.ComponentName]; ok {
					if _, ok2 := acts[depAct]; ok2 {
						depOnly = false
					}
				}
			}
		} else {
			// no required action; treat as orchestration-only unless the dep component has any actions
			if isSameHostFilter(filter) && len(parentHosts) > 0 {
				if hostMap, ok := hasActionByHost[d.ComponentName]; ok {
					for _, h := range parentHosts {
						if acts := hostMap[h]; len(acts) > 0 {
							depOnly = false
							break
						}
					}
				}
			} else {
				if acts, ok := hasAction[d.ComponentName]; ok && len(acts) > 0 {
					depOnly = false
				}
			}
		}

		k := key{
			name:              d.ComponentName,
			filter:            filter,
			rev:               rev,
			curAct:            curAct,
			depAct:            depAct,
			compFilter:        compFilter,
			orchestrationOnly: depOnly,
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}

		out = append(out, DependsOnOut{
			ComponentName:     d.ComponentName,
			ServerFilter:      filter,
			ReverseDependency: rev,
			CurrentAction:     curAct,
			DependsOnAction:   depAct,
			ComponentFilter:   compFilter,
			OrchestrationOnly: depOnly,
		})
	}

	// stable order
	sort.Slice(out, func(i, j int) bool {
		if out[i].ComponentName != out[j].ComponentName {
			return out[i].ComponentName < out[j].ComponentName
		}
		if out[i].ServerFilter != out[j].ServerFilter {
			return out[i].ServerFilter < out[j].ServerFilter
		}
		if out[i].DependsOnAction != out[j].DependsOnAction {
			return out[i].DependsOnAction < out[j].DependsOnAction
		}
		if out[i].CurrentAction != out[j].CurrentAction {
			return out[i].CurrentAction < out[j].CurrentAction
		}
		if out[i].ComponentFilter != out[j].ComponentFilter {
			return out[i].ComponentFilter < out[j].ComponentFilter
		}
		if out[i].ReverseDependency != out[j].ReverseDependency {
			return !out[i].ReverseDependency && out[j].ReverseDependency
		}
		// put orchestrationOnly=false first
		if out[i].OrchestrationOnly != out[j].OrchestrationOnly {
			return !out[i].OrchestrationOnly && out[j].OrchestrationOnly
		}
		return false
	})
	return out
}

// computeLifecycle derives a per-component ordered action list
// STRICT mode: do NOT add missing peers between DOWNLOAD/INSTALL.
// Only include actions that actually appear in the plan for this component.
func computeLifecycle(name string, a *agg, hasAction map[string]map[string]struct{}) []string {
	actsSet := map[string]struct{}{}
	if s, ok := hasAction[name]; ok {
		for k := range s {
			actsSet[k] = struct{}{}
		}
	}

	// Order action list: STOP < DOWNLOAD < INSTALL < START < HEALTH_CHECK < HEALTH
	order := []string{"STOP", "DOWNLOAD", "INSTALL", "START", "HEALTH_CHECK", "HEALTH"}
	var out []string
	for _, aName := range order {
		if _, ok := actsSet[aName]; ok {
			out = append(out, aName)
		}
	}
	return out
}
