package pkg

type TaskRunner interface {
	TaskRunnerRun(task Task) error
}
