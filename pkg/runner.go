package pkg

import (
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// TODO do we really need a type instead of just a function for this?
//   answer: yes, when concurrency gets added
type TaskRunner struct {
	Task Task
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

func (tr *TaskRunner) linearizeTasks(task Task, traversal []Task, done map[string]bool, inProgress map[string]bool, stack []string, taskNamesToIds map[string]string) ([]Task, error) {
	log.Infof("stack: %+v", stack)
	log.Infof("ids: %+v", taskNamesToIds)

	name := task.TaskName()

	// Let's make sure we don't have multiple tasks of the same name.
	//   We'll use the addresses of objects as a unique key.
	//   Whenever we see a name for the first time, we store its address.
	//   Every time thereafter we see that name, we make sure the task address matches that which we stored.
	//   If we see a single name with multiple address, then we have a problem.
	id := fmt.Sprintf("%p", task)
	prevId, ok := taskNamesToIds[name]
	if !ok {
		taskNamesToIds[name] = id
	} else if id != prevId {
		return nil, errors.Errorf("duplicate task name %s detected, ids %s and %s", name, id, prevId)
	}

	// An example where this would happen:
	//   a -> b
	//   b -> c
	//   a -> c
	//   Since a and b both depend on c, we'll traverse c twice.  The first time c is hit, we need to process it.
	//   Subsequent times, just ignore it.  Note: this doesn't mean there's a cycle.
	log.Infof("handling %s\n", name)
	if done[name] {
		log.Infof("bailing on %s\n", name)
		return traversal, nil
	}

	// If we come across a task that we're already processing, then we have a cycle.
	if inProgress[name] {
		return nil, errors.Errorf("cycle detected -- stack %+v, next %s", stack, name)
	}

	// Okay, let's process this task.
	//   We'll add it to our in-progress tasks set for cycle detection, and add it to our
	//   in-progress tasks stack for debugging and error messages.
	inProgress[name] = true
	stack = append(stack, name)

	for _, t := range task.TaskDependencies() {
		var err error
		traversal, err = tr.linearizeTasks(t, traversal, done, inProgress, stack, taskNamesToIds)
		if err != nil {
			return nil, err
		}
	}

	// Clean up the record of this task.  It's no longer in-progress, and it's done.
	//   There's no need to pop `stack` due to go's slice semantics.
	delete(inProgress, name)
	done[name] = true

	// A task can be run after all its dependencies have run, so we'll just append it to the traversal.
	return append(traversal, task), nil
}

// TODO change name to schedule or something?  b/c idea is to do a DFS-order execution of tasks,
//   doesn't necessarily have to be linear (could be parallel)
func (tr *TaskRunner) LinearizeTasks() ([]Task, error) {
	traversal := []Task{}
	done := map[string]bool{}
	inProgress := map[string]bool{}
	taskNamesToIds := map[string]string{}
	return tr.linearizeTasks(tr.Task, traversal, done, inProgress, nil, taskNamesToIds)
}

// TODO add option to pre-validate prereqs for *all* tasks
func (tr *TaskRunner) Run() error {
	tasks, err := tr.LinearizeTasks()
	if err != nil {
		return errors.WithMessagef(err, "unable to linearize tasks under task %s", tr.Task.TaskName())
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
