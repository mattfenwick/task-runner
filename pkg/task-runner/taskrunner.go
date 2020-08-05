package task_runner

type TaskRunner interface {
	TaskRunnerRun(task Task) error
}
