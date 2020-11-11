package task_runner

import "time"

type TaskRunResult struct {
	Task   Task
	State  TaskState
	Start  time.Time
	Finish time.Time
}

func (trr *TaskRunResult) Duration() time.Duration {
	return trr.Finish.Sub(trr.Start)
}
