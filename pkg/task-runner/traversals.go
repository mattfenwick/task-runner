package task_runner

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"strings"
)

func traverseHelp(visited map[string]bool, currentTask Task, depth int, f func(Task, int)) {
	name := currentTask.TaskName()
	if visited[name] {
		log.Debugf("skipping traversal of task %s, already visited", name)
		return
	}
	log.Debugf("traversing task %s at depth %d", name, depth)
	f(currentTask, depth)
	visited[name] = true
	for _, dep := range currentTask.TaskDependencies() {
		traverseHelp(visited, dep, depth+1, f)
	}
}

// TaskTraverse executes a preorder DFS of the task graph.
//   TODO add support for dupe/cycle detection?
func TaskTraverse(t Task, f func(currentTask Task, depth int)) {
	log.Debugf("starting traversal of task %s", t.TaskName())
	traverseHelp(map[string]bool{}, t, 0, f)
}

func TaskDebugPrint(rootTask Task) {
	TaskTraverse(rootTask, func(currentTask Task, level int) {
		fmt.Printf("%s%s: %p, %p\n", strings.Repeat(" ", level*2), currentTask.TaskName(), currentTask, &currentTask)
	})
}

func TaskToGraphDump(rootTask Task) *GraphDump {
	dump := &GraphDump{
		Prereqs: map[string]bool{},
		Tasks:   map[string]*GraphDumpTask{},
	}
	TaskTraverse(rootTask, func(currentTask Task, depth int) {
		name := currentTask.TaskName()
		if _, ok := dump.Tasks[name]; ok {
			return
		}
		var status GraphDumpTaskStatus
		isDone, err := currentTask.TaskIsDone()
		if err != nil {
			status = GraphDumpTaskStatusUnknown
		} else if isDone {
			status = GraphDumpTaskStatusDone
		} else {
			status = GraphDumpTaskStatusReady
		}
		var prereqs []string
		for _, prereq := range currentTask.TaskPrereqs() {
			if _, ok := dump.Prereqs[prereq.PrereqName()]; !ok {
				dump.Prereqs[prereq.PrereqName()] = prereq.PrereqRun() == nil
			}
			if status == GraphDumpTaskStatusReady && !dump.Prereqs[prereq.PrereqName()] {
				status = GraphDumpTaskStatusNotReady
			}
			prereqs = append(prereqs, prereq.PrereqName())
		}
		var deps []string
		for _, dep := range currentTask.TaskDependencies() {
			deps = append(deps, dep.TaskName())
		}
		dump.Tasks[name] = &GraphDumpTask{
			Prereqs: prereqs,
			Status:  status,
			Deps:    deps,
		}
	})
	return dump
}
