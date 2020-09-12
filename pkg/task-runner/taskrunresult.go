package task_runner

import "time"

type TaskRunResult struct {
	Task     Task
	State    TaskState
	Duration time.Duration
}
