package task_runner

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"strings"
)

type GraphDumpTaskStatus string

const (
	GraphDumpTaskStatusNotReady GraphDumpTaskStatus = "NotReady"
	GraphDumpTaskStatusReady    GraphDumpTaskStatus = "Ready"
	GraphDumpTaskStatusDone     GraphDumpTaskStatus = "Done"
	GraphDumpTaskStatusUnknown  GraphDumpTaskStatus = "Unknown"
)

func (gs GraphDumpTaskStatus) Color() string {
	switch gs {
	case GraphDumpTaskStatusNotReady:
		return "red"
	case GraphDumpTaskStatusReady:
		return "yellow"
	case GraphDumpTaskStatusDone:
		return "green"
	case GraphDumpTaskStatusUnknown:
		return "orange" // TODO weird choice, maybe this should be something different?
	default:
		panic(errors.Errorf("invalid GraphDumpTaskStatus: %s", gs))
	}
}

// TODO sort this stuff
type GraphDump struct {
	Prereqs map[string]bool
	Tasks   map[string]*GraphDumpTask
}

type GraphDumpTask struct {
	Prereqs []string
	Status  GraphDumpTaskStatus
	Deps    []string
}

func (g *GraphDump) RenderAsJSON(includePrereqs bool) string {
	if !includePrereqs {
		g.Prereqs = nil
		for _, taskInfo := range g.Tasks {
			taskInfo.Prereqs = nil
		}
	}
	bytes, err := json.MarshalIndent(g, "", "  ")
	if err != nil {
		panic(errors.Wrapf(err, "unable to marshal json (should not have happened)"))
	}
	return string(bytes)
}

func (g *GraphDump) RenderAsList(includePrereqs bool) string {
	var lines []string
	if includePrereqs {
		lines = append(lines, "Prereqs:")
		for prereq, ok := range g.Prereqs {
			status := "ok"
			if !ok {
				status = "not done"
			}
			lines = append(lines, fmt.Sprintf("  %s: %s", prereq, status))
		}
	}
	lines = append(lines, "", "")
	for task, taskInfo := range g.Tasks {
		lines = append(lines, fmt.Sprintf("%s: %s", task, taskInfo.Status))
		if includePrereqs {
			for _, prereq := range taskInfo.Prereqs {
				lines = append(lines, "  (prq): "+prereq)
			}
		}
		for _, dep := range taskInfo.Deps {
			lines = append(lines, "  "+dep)
		}
		lines = append(lines, "")
	}
	return strings.Join(lines, "\n")
}

func (g *GraphDump) RenderAsDot(includePrereqs bool) string {
	lines := []string{`digraph "task-runner" {`}
	if includePrereqs {
		for prereq, isSatisfied := range g.Prereqs {
			color := "red"
			if isSatisfied {
				color = "green"
			}
			lines = append(lines, fmt.Sprintf(`  "%s" [color="%s",penwidth=2,style="dashed"];`, prereq, color))
		}
	}
	for task, taskInfo := range g.Tasks {
		lines = append(lines, fmt.Sprintf(`  "%s" [color=%s,penwidth=5];`, task, taskInfo.Status.Color()))
		if includePrereqs {
			for _, prereq := range taskInfo.Prereqs {
				lines = append(lines, fmt.Sprintf(`  "%s" -> "%s" [style="dashed"];`, task, prereq))
			}
		}
		for _, to := range taskInfo.Deps {
			lines = append(lines, fmt.Sprintf(`  "%s" -> "%s";`, task, to))
		}
	}
	return strings.Join(append(lines, "}"), "\n")
}
