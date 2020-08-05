package task_runner

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
// Tasks cannot be added after execution begins.
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
	log.Debugf("task states: %+v", taskStates)

	if runAllPrereqsImmediately {
		log.Debugf("running %d prereqs immediately", len(prereqs))
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
		log.Debugf("all %d prereqs succeeded", len(prereqs))
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
		log.Debugf("skipping task %s, already done", task.TaskName())
		taskStates[task.TaskName()] = SimpleTaskRunnerTaskStateSkipped
		return nil
	}

	// Make sure all the prerequisites for a task are met.
	for _, p := range task.TaskPrereqs() {
		log.Debugf("checking prereq %s of task %s", p.PrereqName(), task.TaskName())
		if err := p.PrereqRun(); err != nil {
			taskStates[task.TaskName()] = SimpleTaskRunnerTaskStateFailed
			return errors.WithMessagef(err, "prereq '%s' failed for task %s", p.PrereqName(), task.TaskName())
		}
		log.Debugf("prereq %s of task %s is good to go", p.PrereqName(), task.TaskName())
	}

	// Actually run the task.
	log.Debugf("running task %s", task.TaskName())
	err = task.TaskRun()
	if err != nil {
		taskStates[task.TaskName()] = SimpleTaskRunnerTaskStateFailed
		return errors.WithMessagef(err, "failed to run task %s", task.TaskName())
	}

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

	log.Debugf("finished running task %s", task.TaskName())
	taskStates[task.TaskName()] = SimpleTaskRunnerTaskStateComplete
	return nil
}

func linearizeHelp(task Task, traversal []Task, done map[string]bool, inProgress map[string]bool, stack []string, taskNamesToIds map[string]string) ([]Task, error) {
	log.Debugf("stack: %+v", stack)
	log.Debugf("ids: %+v", taskNamesToIds)

	name := task.TaskName()

	// Let's make sure we don't have multiple tasks of the same name.
	//   We'll use the addresses of objects as a unique key.
	//   Whenever we see a name for the first time, we store its address.
	//   Every time thereafter we see that name, we make sure the task address matches that which we stored.
	//   If we see a single name with multiple address, then we have a problem.
	id := ObjectId(task)
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
	log.Debugf("handling %s\n", name)
	if done[name] {
		log.Debugf("bailing on %s\n", name)
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
		traversal, err = linearizeHelp(t, traversal, done, inProgress, stack, taskNamesToIds)
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

func TaskLinearize(task Task) ([]Task, []Prereq, error) {
	traversal := []Task{}
	done := map[string]bool{}
	inProgress := map[string]bool{}
	taskNamesToIds := map[string]string{}
	taskOrder, err := linearizeHelp(task, traversal, done, inProgress, nil, taskNamesToIds)
	if err != nil {
		return nil, nil, err
	}
	prereqs, err := gatherPrereqs(taskOrder)
	if err != nil {
		return nil, nil, err
	}
	return taskOrder, prereqs, nil
}

func gatherPrereqs(tasks []Task) ([]Prereq, error) {
	prereqNameToId := map[string]string{}
	var prereqs []Prereq
	for _, task := range tasks {
		for _, prereq := range task.TaskPrereqs() {
			name := prereq.PrereqName()
			newId := ObjectId(prereq)
			if oldId, ok := prereqNameToId[name]; ok {
				if newId != oldId {
					return nil, errors.Errorf("duplicate prereq name %s for ids %s and %s", name, newId, oldId)
				}
				// otherwise: we've already seen this prereq, but it's okay so just don't add it to `prereqs` again
			} else {
				prereqNameToId[name] = newId
				prereqs = append(prereqs, prereq)
			}
		}
	}
	return prereqs, nil
}
