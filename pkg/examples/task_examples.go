package examples

import (
	"fmt"
	"github.com/mattfenwick/task-runner/pkg/task-runner"
	"github.com/pkg/errors"
	"sync"
)

// PrintTask prints a string and marks itself as done.
func PrintTask(s string, deps ...task_runner.Task) task_runner.Task {
	isDone := false
	return task_runner.NewFunctionTask(s, func() error {
		fmt.Println(s)
		isDone = true
		return nil
	}, deps, []task_runner.Prereq{}, func() (b bool, err error) {
		return isDone, nil
	})
}

// SetKeyTask sets a key in a map, then marks itself as done.
// If the key is already set, it returns an error.
// The purpose of this Task is to track whether it's been executed or not,
// and to enforce that it is not executed more than once.
func SetKeyTask(name string, key string, mux *sync.Mutex, dict map[string]bool, deps ...task_runner.Task) task_runner.Task {
	isDone := false
	return task_runner.NewFunctionTask(fmt.Sprintf("task-setkey-%s", name), func() error {
		if _, ok := dict[key]; ok {
			return errors.Errorf("key %s already in dict", key)
		}
		mux.Lock()
		dict[key] = true
		mux.Unlock()
		isDone = true
		return nil
	}, deps, []task_runner.Prereq{}, func() (b bool, err error) {
		return isDone, nil
	})
}

// SetKeyTaskPrereq is similar to SetKeyTask, but moves the 'key is already set?' check
// into a prereq.
func SetKeyTaskPrereq(name string, key string, dict map[string]bool, deps ...task_runner.Task) task_runner.Task {
	isDone := false
	taskName := fmt.Sprintf("task-setkey-%s", name)
	return task_runner.NewFunctionTask(taskName, func() error {
		dict[key] = true
		isDone = true
		return nil
	}, deps, []task_runner.Prereq{
		&task_runner.FunctionPrereq{
			Name: fmt.Sprintf("prereq-keycheck-%s", taskName),
			Run: func() error {
				if _, ok := dict[key]; ok {
					return errors.Errorf("key %s already in dict", key)
				}
				return nil
			},
		},
	}, func() (b bool, err error) {
		return isDone, nil
	})
}

// SetKeyTask sets a key in a map, then marks itself as done.
// If the key is already set, it will report itself as done.
func SetKeyTaskIdempotent(name string, key string, dict map[string]bool, deps ...task_runner.Task) task_runner.Task {
	return task_runner.NewFunctionTask(fmt.Sprintf("task-setkeyidempotent-%s", name), func() error {
		dict[key] = true
		return nil
	}, deps, []task_runner.Prereq{}, func() (b bool, err error) {
		return dict[key], nil
	})
}

func SetKeyCountTask(name string, key string, dict map[string]bool, deps ...task_runner.Task) *RunCountTask {
	return NewRunCountTask(SetKeyTaskIdempotent(name, key, dict, deps...))
}
