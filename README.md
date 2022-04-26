# task-runner
A pure golang task runner

## Examples

Convert a task graph to a [Dot format image](https://en.wikipedia.org/wiki/DOT_(graph_description_language)):

```go
package main

import (
    eg "github.com/mattfenwick/task-runner/pkg/examples"
    tr "github.com/mattfenwick/task-runner/pkg/task-runner"

    "github.com/goccy/go-graphviz"
    "github.com/goccy/go-graphviz/cgraph"
    "github.com/pkg/errors"
    log "github.com/sirupsen/logrus"
)

func main() {
    task := eg.PrintTask("a", eg.PrintTask("b"))
    graphDump := tr.TaskToGraphDump(task)

    if err := ToDot(graphDump, "my-graph.png", true); err != nil {
        panic(err)
    }
}

func ToDot(graphDump *tr.GraphDump, path string, includePrereqs bool) error {
	gv := graphviz.New()
	gv.SetLayout(graphviz.DOT)

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
	for task, taskInfo := range graphDump.Tasks {
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

	// have to do a separate, 2nd traversal to avoid blowing up graphviz with uncreated nodes
	for task, taskInfo := range graphDump.Tasks {
		for _, dep := range taskInfo.Deps {
			_, err := graph.CreateEdge("", taskNodes[task], taskNodes[dep])
			if err != nil {
				return errors.Wrapf(err, "unable to create edge from task node '%s' to task node '%s'", task, dep)
			}
		}
	}

	if err := gv.RenderFilename(graph, graphviz.PNG, path); err != nil {
		return errors.Wrapf(err, "unable to write graph to file '%s'", path)
	}

	return nil
}
```
