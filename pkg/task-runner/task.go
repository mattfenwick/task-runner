package task_runner

import (
	"encoding/json"
	"os/exec"
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

	// For convenient JSON serialization
	TaskJSONObject() map[string]interface{}
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

func (ft *FunctionTask) TaskJSONObject() map[string]interface{} {
	var deps []string
	for _, dep := range ft.Deps {
		deps = append(deps, dep.TaskName())
	}
	var prereqs []string
	for _, p := range ft.Prereqs {
		prereqs = append(prereqs, p.PrereqName())
	}
	return map[string]interface{}{
		"Run":          "[function]",
		"Dependencies": deps,
		"IsDone":       "[function]",
		"Prereqs":      prereqs,
		"Name":         ft.Name,
	}
}

func (ft *FunctionTask) MarshalJSON() ([]byte, error) {
	return json.Marshal(ft.TaskJSONObject())
}

func NewShellTask(name string, cmd *exec.Cmd, deps []Task, prereqs []Prereq, isDone func() (bool, error)) *FunctionTask {
	run := func() error {
		return RunCommandAndPrint(cmd)
	}
	return NewFunctionTask(name, run, deps, prereqs, isDone)
}

func NewRunOnceTask(name string, run func() error, deps []Task, prereqs []Prereq) *FunctionTask {
	isDone := false
	return &FunctionTask{
		Name: name,
		Run: func() error {
			err := run()
			if err != nil {
				return err
			}
			isDone = true
			return nil
		},
		Deps:    deps,
		Prereqs: prereqs,
		IsDone: func() (bool, error) {
			return isDone, nil
		},
	}
}

// NewNoopTask returns a RunOnceTask that doesn't do anything.  This can be used for grouping
// tasks into dependency graphs in a convenient way.
func NewNoopTask(name string, deps []Task) *FunctionTask {
	return NewRunOnceTask(
		name,
		func() error { return nil },
		deps,
		[]Prereq{})
}
