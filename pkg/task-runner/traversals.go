package task_runner

import (
	"fmt"
	"github.com/pkg/errors"
	"strings"
)

func traverseHelp(currentTask Task, depth int, f func(Task, int)) {
	f(currentTask, depth)
	for _, dep := range currentTask.TaskDependencies() {
		traverseHelp(dep, depth+1, f)
	}
}

// TaskTraverse executes a preorder DFS of the task graph.
//   TODO add support for dupe/cycle detection?
func TaskTraverse(t Task, f func(currentTask Task, depth int)) {
	traverseHelp(t, 0, f)
}

func TaskDebugPrint(rootTask Task) {
	TaskTraverse(rootTask, func(currentTask Task, level int) {
		fmt.Printf("%s%s: %p, %p\n", strings.Repeat(" ", level*2), currentTask.TaskName(), currentTask, &currentTask)
	})
}

type GraphDotTaskStatus string

const (
	GraphDotTaskStatusNotReady GraphDotTaskStatus = "NotReady"
	GraphDotTaskStatusReady    GraphDotTaskStatus = "Ready"
	GraphDotTaskStatusDone     GraphDotTaskStatus = "Done"
	GraphDotTaskStatusUnknown  GraphDotTaskStatus = "Unknown"
)

func (gs GraphDotTaskStatus) Color() string {
	switch gs {
	case GraphDotTaskStatusNotReady:
		return "red"
	case GraphDotTaskStatusReady:
		return "yellow"
	case GraphDotTaskStatusDone:
		return "green"
	case GraphDotTaskStatusUnknown:
		return "orange" // TODO weird choice
	default:
		panic(errors.Errorf("invalid GraphDotTaskStatus: %s", gs))
	}
}

type GraphDotDump struct {
	Prereqs map[string]bool
	Tasks   map[string]*GraphDotTask
}

type GraphDotTask struct {
	Prereqs []string
	Status  GraphDotTaskStatus
	Deps    []string
}

func (g *GraphDotDump) RenderAsList() string {
	lines := []string{
		"Prereqs:",
	}
	for prereq, ok := range g.Prereqs {
		status := "ok"
		if !ok {
			status = "not done"
		}
		lines = append(lines, fmt.Sprintf("  %s: %s", prereq, status))
	}
	lines = append(lines, "")
	for task, taskInfo := range g.Tasks {
		lines = append(lines, fmt.Sprintf("%s: %s", task, taskInfo.Status))
		for _, dep := range taskInfo.Deps {
			// TODO should we repeat prereqs here as well?
			lines = append(lines, "  "+dep)
		}
	}
	return strings.Join(lines, "\n")
}

func (g *GraphDotDump) RenderAsDot() string {
	lines := []string{`digraph "task-runner" {`}
	for prereq, isSatisfied := range g.Prereqs {
		color := "red"
		if isSatisfied {
			color = "green"
		}
		lines = append(lines, fmt.Sprintf(`  "%s" [color="%s"];`, prereq, color))
	}
	for task, taskInfo := range g.Tasks {
		lines = append(lines, fmt.Sprintf(`  "%s" [color=%s];`, task, taskInfo.Status.Color()))
		for _, prereq := range taskInfo.Prereqs {
			lines = append(lines, fmt.Sprintf(`  "%s" -> "%s";`, task, prereq))
		}
		for _, to := range taskInfo.Deps {
			lines = append(lines, fmt.Sprintf(`  "%s" -> "%s";`, task, to))
		}
	}
	return strings.Join(append(lines, "}"), "\n")
}

func TaskToDotFormat(rootTask Task) *GraphDotDump {
	dump := &GraphDotDump{
		Prereqs: map[string]bool{},
		Tasks:   map[string]*GraphDotTask{},
	}
	TaskTraverse(rootTask, func(currentTask Task, depth int) {
		name := currentTask.TaskName()
		if _, ok := dump.Tasks[name]; ok {
			return
		}
		var status GraphDotTaskStatus
		isDone, err := currentTask.TaskIsDone()
		if err != nil {
			status = GraphDotTaskStatusUnknown
		} else if isDone {
			status = GraphDotTaskStatusDone
		} else {
			status = GraphDotTaskStatusReady
		}
		var prereqs []string
		for _, prereq := range currentTask.TaskPrereqs() {
			if _, ok := dump.Prereqs[prereq.PrereqName()]; !ok {
				dump.Prereqs[prereq.PrereqName()] = prereq.PrereqRun() == nil
			}
			if status == GraphDotTaskStatusReady && !dump.Prereqs[prereq.PrereqName()] {
				status = GraphDotTaskStatusNotReady
			}
			prereqs = append(prereqs, prereq.PrereqName())
		}
		var deps []string
		for _, dep := range currentTask.TaskDependencies() {
			deps = append(deps, dep.TaskName())
		}
		dump.Tasks[name] = &GraphDotTask{
			Prereqs: prereqs,
			Status:  status,
			Deps:    deps,
		}
	})
	return dump
}
