package pkg

import (
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"strings"
)

type SimpleTaskRunnerTaskState string

const (
	// SimpleTaskRunner just puts tasks in order -- it doesn't actually know if a task is ready or not,
	//   so it has no way to distinguish between 'Ready' and 'Waiting
	//SimpleTaskRunnerTaskStateReady SimpleTaskRunnerTaskState = "SimpleTaskRunnerTaskStateReady"
	// Similarly, since SimpleTaskRunner is single-threaded, there's no point in tracking whether a task
	//   is in progress.  However, if another thread were able to observe execution progress, then this
	//   state could be worth re-adding.
	//SimpleTaskRunnerTaskStateInProgress SimpleTaskRunnerTaskState = "SimpleTaskRunnerTaskStateInProgress"
	SimpleTaskRunnerTaskStateWaiting  SimpleTaskRunnerTaskState = "SimpleTaskRunnerTaskStateWaiting"
	SimpleTaskRunnerTaskStateFailed   SimpleTaskRunnerTaskState = "SimpleTaskRunnerTaskStateFailed"
	SimpleTaskRunnerTaskStateSkipped  SimpleTaskRunnerTaskState = "SimpleTaskRunnerTaskStateSkipped"
	SimpleTaskRunnerTaskStateComplete SimpleTaskRunnerTaskState = "SimpleTaskRunnerTaskStateComplete"
)

func (s SimpleTaskRunnerTaskState) String() string {
	switch s {
	case SimpleTaskRunnerTaskStateWaiting:
		return "Waiting"
	case SimpleTaskRunnerTaskStateFailed:
		return "Failed"
	case SimpleTaskRunnerTaskStateSkipped:
		return "Skipped"
	case SimpleTaskRunnerTaskStateComplete:
		return "Complete"
	default:
		panic(errors.Errorf("invalid SimpleTaskRunnerTaskState value %s", string(s)))
	}
}

// SimpleTaskRunner performs a DFS on a task graph to find a valid execution order, then
// executes tasks one at a time.  It does not execute tasks in parallel.  It does not
// use goroutines.
// SimpleTaskRunner execution fails if:
//   - there are duplicate Tasks (where duplicate = two different Tasks with the same name)
//   - there are duplicate Prereqs (where duplicate = two different Prereqs with the same name)
//   - the task graph is not a DAG -- i.e. there is 1 or more cycles
//   - a Task is unable to report whether it is Done before execution
//   - a Prereq of a Task fails
//   - if after running a Task, the Task reports itself as not Done.  SimpleTaskRunner expects that
//     Tasks report themselves as Done after execution.
// Since SimpleTaskRunner returns a map of Task name to status, if execution fails, the caller
//   can inspect the Task status map to determine which Tasks were run and which failed.
type SimpleTaskRunner struct{}

func (tr *SimpleTaskRunner) TaskRunnerRun(task Task, runAllPrereqsImmediately bool) (map[string]SimpleTaskRunnerTaskState, error) {
	taskOrder, prereqs, err := TaskLinearize(task)
	if err != nil {
		return nil, errors.WithMessagef(err, "unable to linearize tasks under task %s", task.TaskName())
	}

	taskStates := map[string]SimpleTaskRunnerTaskState{}
	for _, t := range taskOrder {
		taskStates[t.TaskName()] = SimpleTaskRunnerTaskStateWaiting
	}

	if runAllPrereqsImmediately {
		log.Infof("running %d prereqs immediately", len(prereqs))
		errs := []string{}
		for _, p := range prereqs {
			log.Debugf("running prereq %s", p.PrereqName())
			err := p.PrereqRun()
			if err != nil {
				errs = append(errs, err.Error())
			}
		}
		if len(errs) > 0 {
			return taskStates, errors.Errorf("%d prereqs failed: [%s]", len(errs), strings.Join(errs, ", "))
		}
		log.Infof("all %d prereqs succeeded", len(prereqs))
	}

	for _, task := range taskOrder {
		err = tr.runTask(task, taskStates)
		if err != nil {
			return taskStates, err
		}
	}
	return taskStates, nil
}

func (tr *SimpleTaskRunner) runTask(task Task, taskStates map[string]SimpleTaskRunnerTaskState) error {
	// If a task is already done, then don't run it.
	isDone, err := task.TaskIsDone()
	if err != nil {
		taskStates[task.TaskName()] = SimpleTaskRunnerTaskStateFailed
		return err
	}
	if isDone {
		log.Infof("skipping task %s, already done", task.TaskName())
		taskStates[task.TaskName()] = SimpleTaskRunnerTaskStateSkipped
		return nil
	}

	// Make sure all the prerequisites for a task are met.
	for _, p := range task.TaskPrereqs() {
		log.Infof("checking prereq %s of task %s", p.PrereqName(), task.TaskName())
		if err := p.PrereqRun(); err != nil {
			taskStates[task.TaskName()] = SimpleTaskRunnerTaskStateFailed
			return errors.WithMessagef(err, "prereq '%s' failed for task %s", p.PrereqName(), task.TaskName())
		}
		log.Infof("prereq %s of task %s is good to go", p.PrereqName(), task.TaskName())
	}

	// Actually run the task.
	log.Infof("running task %s", task.TaskName())
	err = task.TaskRun()
	if err != nil {
		taskStates[task.TaskName()] = SimpleTaskRunnerTaskStateFailed
		return errors.WithMessagef(err, "failed to run task %s", task.TaskName())
	}
	log.Infof("finished running task %s", task.TaskName())

	// After running a task, make sure that it considers itself to be done.
	isDone, err = task.TaskIsDone()
	if err != nil {
		taskStates[task.TaskName()] = SimpleTaskRunnerTaskStateFailed
		return errors.WithMessagef(err, "task %s failed post-execution IsDone check", task.TaskName())
	}
	if !isDone {
		taskStates[task.TaskName()] = SimpleTaskRunnerTaskStateFailed
		return errors.Errorf("ran task %s but it still reports itself as not done", task.TaskName())
	}

	taskStates[task.TaskName()] = SimpleTaskRunnerTaskStateComplete
	return nil
}
