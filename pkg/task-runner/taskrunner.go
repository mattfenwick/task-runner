package task_runner

type TaskRunner interface {
	TaskRunnerRun(task Task) (map[string]*TaskRunResult, error)
}
