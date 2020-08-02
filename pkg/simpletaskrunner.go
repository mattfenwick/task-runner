package pkg

import (
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type SimpleTaskRunnerTaskState string

const (
	SimpleTaskRunnerTaskStateReady SimpleTaskRunnerTaskState = "SimpleTaskRunnerTaskStateReady"
)

type SimpleTaskRunner struct{}

// TODO add option to pre-validate prereqs for *all* tasks
// TODO if there's an error, report all tasks that were and weren't executed
//   maybe return an annotated graph of progress?
func (tr *SimpleTaskRunner) TaskRunnerRun(task Task) error {
	tasks, err := TaskLinearize(task)
	if err != nil {
		return errors.WithMessagef(err, "unable to linearize tasks under task %s", task.TaskName())
	}
	for _, task := range tasks {
		// If a task is already done, then don't run it.
		isDone, err := task.TaskIsDone()
		if err != nil {
			return err
		}
		if isDone {
			log.Infof("skipping task %s, already done", task.TaskName())
			continue
		}

		// Make sure all the prerequisites for a task are met.
		for _, p := range task.TaskPrereqs() {
			log.Infof("checking prereq %s of task %s", p.PrereqName(), task.TaskName())
			if err := p.PrereqRun(); err != nil {
				return errors.WithMessagef(err, "prereq '%s' failed for task %s", p.PrereqName(), task.TaskName())
			}
			log.Infof("prereq %s of task %s is good to go", p.PrereqName(), task.TaskName())
		}

		// Actually run the task.
		log.Infof("running task %s", task.TaskName())
		err = task.TaskRun()
		if err != nil {
			return errors.WithMessagef(err, "failed to run task %s", task.TaskName())
		}
		log.Infof("finished running task %s", task.TaskName())

		// After running a task, make sure that it considers itself to be done.
		isDone, err = task.TaskIsDone()
		if err != nil {
			return err
		}
		if !isDone {
			return errors.Errorf("ran task %s but it still reports itself as not done", task.TaskName())
		}
	}
	return nil
}
