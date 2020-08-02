package pkg

import (
	"fmt"
	"os/exec"
	"strings"
)

type Task interface {
	TaskName() string
	TaskDependencies() []Task
	TaskPrereqs() []Prereq
	TaskRun() error
	TaskIsDone() (bool, error)

	// TODO should this add error-checking for:
	//   adding a dependency that's already there (probably yes)
	//   adding a dependency to itself (not sure)
	TaskAddDependency(dep Task)
}

func traverseHelp(currentTask Task, depth int, f func(Task, int)) {
	f(currentTask, depth)
	for _, dep := range currentTask.TaskDependencies() {
		traverseHelp(dep, depth+1, f)
	}
}

// TaskTraverse executes a preorder DFS of the task graph.
//   TODO add support for dupe/cycle detection?
func TaskTraverse(t Task, f func(Task, int)) {
	traverseHelp(t, 0, f)
}

func TaskDebugPrint(rootTask Task) {
	TaskTraverse(rootTask, func(currentTask Task, level int) {
		fmt.Printf("%s%s: %p, %p\n", strings.Repeat(" ", level*2), currentTask.TaskName(), currentTask, &currentTask)
	})
}

type FunctionTask struct {
	Name    string
	Run     func() error
	Deps    []Task
	Prereqs []Prereq
	IsDone  func() (bool, error)
}

func NewFunctionTask(name string, run func() error, deps []Task, prereqs []Prereq, isDone func() (bool, error)) *FunctionTask {
	return &FunctionTask{
		Name:    name,
		Run:     run,
		Deps:    deps,
		Prereqs: prereqs,
		IsDone:  isDone,
	}
}

func (ft *FunctionTask) TaskName() string {
	return ft.Name
}

func (ft *FunctionTask) TaskDependencies() []Task {
	return ft.Deps
}

func (ft *FunctionTask) TaskPrereqs() []Prereq {
	return ft.Prereqs
}

func (ft *FunctionTask) TaskRun() error {
	return ft.Run()
}

func (ft *FunctionTask) TaskIsDone() (bool, error) {
	return ft.IsDone()
}

func (ft *FunctionTask) TaskAddDependency(dep Task) {
	ft.Deps = append(ft.Deps, dep)
}

func NewShellTask(name string, cmd *exec.Cmd, deps []Task, prereqs []Prereq, isDone func() (bool, error)) *FunctionTask {
	run := func() error {
		return RunCommandAndPrint(cmd)
	}
	return NewFunctionTask(name, run, deps, prereqs, isDone)
}
