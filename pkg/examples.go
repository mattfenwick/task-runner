package pkg

import (
	"fmt"
	"github.com/pkg/errors"
	"sync/atomic"
)

// PrintTask prints a string and marks itself as done.
func PrintTask(s string, deps ...Task) Task {
	isDone := false
	return NewFunctionTask(fmt.Sprintf("task-print-%s", s), func() error {
		fmt.Println(s)
		isDone = true
		return nil
	}, deps, []Prereq{}, func() (b bool, err error) {
		return isDone, nil
	})
}

// SetKeyTask sets a key in a map, then marks itself as done.
// If the key is already set, it returns an error.
// The purpose of this Task is to track whether it's been executed or not,
// and to enforce that it is not executed more than once.
// TODO should there be a Prereq that key is not present in the map?
//   Or should isDone just return dict[key] ?
//   A: no and no, b/c this is a special case for testing
func SetKeyTask(name string, key string, dict map[string]bool, deps ...Task) Task {
	isDone := false
	return NewFunctionTask(fmt.Sprintf("task-setkey-%s", name), func() error {
		if _, ok := dict[key]; ok {
			return errors.Errorf("key %s already in dict", key)
		}
		dict[key] = true
		isDone = true
		return nil
	}, deps, []Prereq{}, func() (b bool, err error) {
		return isDone, nil
	})
}

// SetKeyTask sets a key in a map, then marks itself as done.
// If the key is already set, it will report itself as done.
func SetKeyTaskIdempotent(name string, key string, dict map[string]bool, deps ...Task) Task {
	return NewFunctionTask(fmt.Sprintf("task-setkeyidempotent-%s", name), func() error {
		dict[key] = true
		return nil
	}, deps, []Prereq{}, func() (b bool, err error) {
		return dict[key], nil
	})
}

type RunCountTask struct {
	RunCount    int64
	WrappedTask Task
}

func NewRunCountTask(task Task) *RunCountTask {
	return &RunCountTask{
		RunCount:    0,
		WrappedTask: task,
	}
}

func (rt *RunCountTask) TaskName() string {
	return fmt.Sprintf("RunCountTask-wrapper-%s", rt.WrappedTask.TaskName())
}

func (rt *RunCountTask) TaskDependencies() []Task {
	return rt.WrappedTask.TaskDependencies()
}

func (rt *RunCountTask) TaskPrereqs() []Prereq {
	return rt.WrappedTask.TaskPrereqs()
}

func (rt *RunCountTask) TaskRun() error {
	atomic.AddInt64(&rt.RunCount, 1)
	return rt.WrappedTask.TaskRun()
}

func (rt *RunCountTask) TaskIsDone() (bool, error) {
	return rt.WrappedTask.TaskIsDone()
}

func (rt *RunCountTask) TaskAddDependency(dep Task) {
	rt.WrappedTask.TaskAddDependency(dep)
}
