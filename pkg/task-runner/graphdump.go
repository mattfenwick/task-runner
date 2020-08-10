package task_runner

import (
	"encoding/json"
	"fmt"
	"github.com/goccy/go-graphviz"
	"github.com/goccy/go-graphviz/cgraph"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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

func (g *GraphDump) ToDot(path string, format graphviz.Format, includePrereqs bool) error {
	return g.ToDotWithLayout(path, format, graphviz.DOT, includePrereqs)
}

func (g *GraphDump) ToDotWithLayout(path string, format graphviz.Format, layout graphviz.Layout, includePrereqs bool) error {
	gv := graphviz.New()
	gv.SetLayout(layout)

	graph, err := gv.Graph()
	if err != nil {
		return errors.Wrapf(err, "unable to instantiate graphviz graph")
	}
	defer func() {
		if err := graph.Close(); err != nil {
			log.Fatalf("%+v", errors.Wrapf(err, "unable to close graph"))
		}
		if err := gv.Close(); err != nil {
			log.Fatalf("%+v", errors.Wrapf(err, "unable to close graphviz"))
		}
	}()

	prereqNodes := map[string]*cgraph.Node{}
	if includePrereqs {
		for prereq, isSatisfied := range g.Prereqs {
			color := "red"
			if isSatisfied {
				color = "green"
			}
			node, err := graph.CreateNode(prereq)
			if err != nil {
				return errors.Wrapf(err, "unable to create prereq node '%s'", prereq)
			}
			node.SetColor(color)
			node.SetPenWidth(2)
			node.SetStyle(cgraph.DashedNodeStyle)
			prereqNodes[prereq] = node
		}
	}
	taskNodes := map[string]*cgraph.Node{}
	for task, taskInfo := range g.Tasks {
		node, err := graph.CreateNode(task)
		if err != nil {
			return errors.Wrapf(err, "unable to create task node '%s'", task)
		}
		node.SetColor(taskInfo.Status.Color())
		node.SetPenWidth(5)
		if includePrereqs {
			for _, prereq := range taskInfo.Prereqs {
				edge, err := graph.CreateEdge("", node, prereqNodes[prereq])
				if err != nil {
					return errors.Wrapf(err, "unable to create edge from task node '%s' to prereq node '%s'", task, prereq)
				}
				edge.SetStyle(cgraph.DashedEdgeStyle)
			}
		}
		taskNodes[task] = node
	}

	// have to do a separate, 2nd traversal to avoid blowing up graphviz
	for task, taskInfo := range g.Tasks {
		for _, dep := range taskInfo.Deps {
			_, err := graph.CreateEdge("", taskNodes[task], taskNodes[dep])
			if err != nil {
				return errors.Wrapf(err, "unable to create edge from task node '%s' to task node '%s'", task, dep)
			}
		}
	}

	//var buf bytes.Buffer
	//if err := gv.Render(graph, "dot", &buf); err != nil {
	//	return errors.Wrapf(err, "unable to render")
	//}
	//fmt.Println(buf.String())

	if err := gv.RenderFilename(graph, format, path); err != nil {
		return errors.Wrapf(err, "unable to write graph to file '%s'", path)
	}

	return nil
}
