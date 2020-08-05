package examples

import (
	"encoding/json"
	"fmt"
	task_runner "github.com/mattfenwick/task-runner/pkg/task-runner"
	"sync/atomic"
)

type RunCountTask struct {
	RunCount    int64
	WrappedTask task_runner.Task
}

func NewRunCountTask(task task_runner.Task) *RunCountTask {
	return &RunCountTask{
		RunCount:    0,
		WrappedTask: task,
	}
}

func (rt *RunCountTask) TaskName() string {
	return fmt.Sprintf("RunCountTask-wrapper-%s", rt.WrappedTask.TaskName())
}

func (rt *RunCountTask) TaskDependencies() []task_runner.Task {
	return rt.WrappedTask.TaskDependencies()
}

func (rt *RunCountTask) TaskPrereqs() []task_runner.Prereq {
	return rt.WrappedTask.TaskPrereqs()
}

func (rt *RunCountTask) TaskRun() error {
	atomic.AddInt64(&rt.RunCount, 1)
	return rt.WrappedTask.TaskRun()
}

func (rt *RunCountTask) TaskIsDone() (bool, error) {
	return rt.WrappedTask.TaskIsDone()
}

func (rt *RunCountTask) TaskAddDependency(dep task_runner.Task) {
	rt.WrappedTask.TaskAddDependency(dep)
}

func (rt *RunCountTask) TaskJSONObject() map[string]interface{} {
	return map[string]interface{}{
		"RunCount":    rt.RunCount,
		"WrappedTask": rt.WrappedTask.TaskJSONObject(),
	}
}

func (rt *RunCountTask) MarshalJSON() ([]byte, error) {
	return json.Marshal(rt.TaskJSONObject())
}
