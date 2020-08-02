package pkg

import "github.com/pkg/errors"

type ParallelTaskRunner struct {
	//Task Task
	// TODO track task state in a map.  States:
	//   WaitingForDependencies
	//   Ready
	//   Running
	//   Complete
	//   Failed
	//   Cancelled (by caller action)
	//   FailedDependency (did not run b/c deps failed)
	// TODO use this to track what directly depends on each task
	//   whenever a task `X` is finished, all of the tasks that depend on `X` get `X` removed from their list
	//   if any of those end up with 0 unfulfilled dependencies, then they get added to 'Ready' queue
	//   if `X` fails, all of the tasks depending on `X` move to FailedDependency and can not run
	// TaskDeps map[string]map[string]string
}

func (ptr *ParallelTaskRunner) TaskRunnerRun(task Task) error {
	return errors.Errorf("TODO")
}
