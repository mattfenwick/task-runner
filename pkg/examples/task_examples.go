package examples

import (
	"fmt"
	"github.com/mattfenwick/task-runner/pkg/task-runner"
	"github.com/pkg/errors"
	"sync"
	"time"
)

// PrintTask is a run-once-task that prints a string.
func PrintTask(s string, deps ...task_runner.Task) task_runner.Task {
	return task_runner.NewRunOnceTask(s, func() error {
		fmt.Println(s)
		return nil
	}, deps, []task_runner.Prereq{})
}

// SetKeyTask is a run-once-task that sets a key in a map.
// If the key is already set, it returns an error.
func SetKeyTask(name string, key string, mux *sync.Mutex, dict map[string]bool, sleepSeconds int, deps ...task_runner.Task) task_runner.Task {
	return task_runner.NewRunOnceTask(fmt.Sprintf("task-setkey-%s", name), func() error {
		if _, ok := dict[key]; ok {
			return errors.Errorf("key %s already in dict", key)
		}
		if sleepSeconds > 0 {
			time.Sleep(time.Second * time.Duration(sleepSeconds))
		}
		mux.Lock()
		dict[key] = true
		mux.Unlock()
		return nil
	}, deps, []task_runner.Prereq{})
}

// SetKeyTaskPrereq is similar to SetKeyTask, but moves the 'key is already set?' check
// into a prereq.
func SetKeyTaskPrereq(name string, key string, dict map[string]bool, deps ...task_runner.Task) task_runner.Task {
	taskName := fmt.Sprintf("task-setkey-%s", name)
	return task_runner.NewRunOnceTask(taskName, func() error {
		dict[key] = true
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
