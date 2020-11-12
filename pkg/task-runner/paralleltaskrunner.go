package task_runner

import (
	"context"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type RunnerTask struct {
	State  TaskState
	Task   Task
	Id     string
	Start  time.Time
	Finish time.Time
	// UpstreamDeps: what unfinished tasks do I depend on?
	// When the number of UpstreamDeps goes to 0, the Task is ready to run and is added to the `Ready` queue.
	UpstreamDeps map[string]bool
	// DownstreamDeps: what tasks are waiting for me to finish?
	DownstreamDeps map[string]bool
}

func (rt *RunnerTask) Duration() time.Duration {
	return rt.Finish.Sub(rt.Start)
}

type ParallelTaskRunnerState string

const (
	ParallelTaskRunnerStateRunning  ParallelTaskRunnerState = "ParallelTaskRunnerStateRunning"
	ParallelTaskRunnerStateStopped  ParallelTaskRunnerState = "ParallelTaskRunnerStateStopped"
	ParallelTaskRunnerStateFinished ParallelTaskRunnerState = "ParallelTaskRunnerStateFinished"
)

type ParallelTaskRunner struct {
	State           ParallelTaskRunnerState
	Tasks           map[string]*RunnerTask
	Concurrency     int
	stopChan        chan struct{}
	actions         chan func()
	readyTasks      chan string
	enqueuedTasks   map[string]bool
	inprogressTasks map[string]bool
}

func NewDefaultParallelTaskRunner(task Task, concurrency int) (*ParallelTaskRunner, error) {
	return NewParallelTaskRunner(task, concurrency, 1000)
}

func NewParallelTaskRunner(task Task, concurrency int, queueSize int) (*ParallelTaskRunner, error) {
	runner := &ParallelTaskRunner{
		State:           ParallelTaskRunnerStateRunning,
		Tasks:           map[string]*RunnerTask{},
		Concurrency:     concurrency,
		stopChan:        make(chan struct{}),
		actions:         make(chan func()),
		readyTasks:      make(chan string, queueSize),
		enqueuedTasks:   map[string]bool{},
		inprogressTasks: map[string]bool{},
	}
	err := runner.addTask(task)
	if err != nil {
		return nil, err
	}

	go func() {
		log.Debugf("starting ParallelTaskRunner action processor")
		for {
			var action func()
			select {
			// TODO this goroutine is potentially leaked -- the problem is, if stopped, how to wait for
			//   every in-progress task to finish before exiting this goroutine?
			//case <-runner.stopChan:
			//	log.Debugf("stopping ParallelTaskRunner action processor")
			//	return
			case action = <-runner.actions:
			}

			// TODO metrics, logging, error reporting
			action()
		}
	}()
	for i := 0; i < runner.Concurrency; i++ {
		runner.createWorker()
	}
	return runner, nil
}

func (runner *ParallelTaskRunner) Stop() error {
	return runner.stopAction()
}

func (runner *ParallelTaskRunner) Wait(ctx context.Context) map[string]*RunnerTask {
	select {
	case <-ctx.Done():
		return nil
	case <-runner.stopChan:
	}
	return runner.Tasks
}

func (runner *ParallelTaskRunner) addTask(task Task) error {
	// 1. build the task tables
	//tasks, err := BuildDependencyTables(task)
	tasks, err := BuildDependencyTablesIterative(task)
	if err != nil {
		return err
	}
	// 2. add in the tasks
	for name, runnerTask := range tasks {
		runner.Tasks[name] = runnerTask
		// no upstream dependencies?  ready to execute, queue it up!
		if len(runnerTask.UpstreamDeps) == 0 {
			runner.setTaskState(runnerTask, TaskStateReady)
		}
	}

	return nil
}

func (runner *ParallelTaskRunner) createWorker() {
	go func() {
		log.Debugf("starting ParallelTaskRunner worker")
		for {
			var taskName string
			select {
			case <-runner.stopChan:
				log.Debugf("stopping ParallelTaskRunner worker")
				return
			case taskName = <-runner.readyTasks:
			}

			runner.runTask(taskName)
		}
	}()
}

// 'actions' are serialized on a single goroutine, and should be the only things allowed to modify
// internal state

// startTask synchronously moves the task state from Ready -> InProgress and returns the
// underlying Task object
func (runner *ParallelTaskRunner) startTaskAction(taskName string) Task {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	var task Task
	runner.actions <- func() {
		runnerTask := runner.Tasks[taskName]
		if runnerTask.State != TaskStateReady {
			panic(errors.Errorf("task %s in invalid state, expected Ready, got %s", taskName, runnerTask.State.String()))
		}

		runner.setTaskState(runnerTask, TaskStateInProgress)
		task = runnerTask.Task
		wg.Done()
	}
	wg.Wait()
	return task
}

func (runner *ParallelTaskRunner) didFinishTaskAction(taskName string, state TaskState, err error, start time.Time, finish time.Time) {
	if err != nil {
		// TODO anything else to do here?
		log.Errorf("failed to run task %s: %+v", taskName, err)
	} else {
		if !(state == TaskStateSkipped || state == TaskStateComplete) {
			panic(errors.Errorf("expected task state Skipped or Complete, found %s", state.String()))
		}
	}

	// Any tasks that depend on this one, now have one less dependency blocking their execution.
	// If anything gets down to 0, queue it up!
	wg := &sync.WaitGroup{}
	wg.Add(1)
	runner.actions <- func() {
		runnerTask := runner.Tasks[taskName]
		runner.setTaskState(runnerTask, state)
		runnerTask.Start = start
		runnerTask.Finish = finish

		if runner.State == ParallelTaskRunnerStateRunning {
			if state == TaskStateComplete || state == TaskStateSkipped {
				for downstreamName := range runnerTask.DownstreamDeps {
					downstream := runner.Tasks[downstreamName]
					delete(downstream.UpstreamDeps, taskName)
					if len(downstream.UpstreamDeps) == 0 {
						if downstream.State != TaskStateWaiting {
							panic(errors.Errorf("expected state Waiting for task %s, found %s", downstreamName, downstream.State.String()))
						}
						runner.setTaskState(downstream, TaskStateReady)
					}
				}
			}

			if len(runner.inprogressTasks) == 0 && len(runner.enqueuedTasks) == 0 {
				runner.State = ParallelTaskRunnerStateFinished
				close(runner.stopChan)
			}
		}

		wg.Done()
	}
	wg.Wait()
}

func (runner *ParallelTaskRunner) stopAction() error {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	var err error
	runner.actions <- func() {
		if runner.State == ParallelTaskRunnerStateRunning {
			runner.State = ParallelTaskRunnerStateStopped
			close(runner.stopChan)
		} else {
			err = errors.Errorf("expected state Running, found state %s", runner.State)
		}
		wg.Done()
	}
	wg.Wait()
	return err
}

// running a task

func runTaskHelper(task Task) (TaskState, error) {
	taskName := task.TaskName()

	// If a task is already done, then don't run it.
	isDone, err := task.TaskIsDone()
	if err != nil {
		return TaskStateFailed, err
	}
	if isDone {
		log.Debugf("skipping task %s, already done", taskName)
		return TaskStateSkipped, nil
	}

	// Make sure all the prerequisites for a task are met.
	for _, p := range task.TaskPrereqs() {
		log.Debugf("checking prereq %s of task %s", p.PrereqName(), taskName)
		if err := p.PrereqRun(); err != nil {
			return TaskStateFailed, errors.WithMessagef(err, "prereq '%s' failed for task %s", p.PrereqName(), taskName)
		}
		log.Debugf("prereq %s of task %s is good to go", p.PrereqName(), taskName)
	}

	// Actually run the task.
	log.Debugf("running task %s", taskName)
	err = task.TaskRun()
	if err != nil {
		return TaskStateFailed, errors.WithMessagef(err, "failed to run task %s", taskName)
	}
	log.Debugf("finished running task %s", taskName)

	// After running a task, make sure that it considers itself to be done.
	isDone, err = task.TaskIsDone()
	if err != nil {
		return TaskStateFailed, errors.WithMessagef(err, "task %s failed post-execution IsDone check", taskName)
	}
	if !isDone {
		return TaskStateFailed, errors.Errorf("ran task %s but it still reports itself as not done", taskName)
	}

	return TaskStateComplete, nil
}

func (runner *ParallelTaskRunner) runTask(taskName string) {
	task := runner.startTaskAction(taskName)

	start := time.Now()
	state, err := runTaskHelper(task)
	finish := time.Now()
	runner.didFinishTaskAction(taskName, state, err, start, finish)
}

// not thread safe: be careful what calls these

func (runner *ParallelTaskRunner) setTaskState(task *RunnerTask, state TaskState) {
	name := task.Task.TaskName()
	log.Debugf("setting task %s state to %s", name, state.String())

	// cleanup on exiting previous state
	switch task.State {
	case TaskStateWaiting:
	case TaskStateReady:
		delete(runner.enqueuedTasks, name)
	case TaskStateInProgress:
		delete(runner.inprogressTasks, name)
	case TaskStateFailed:
	case TaskStateSkipped:
	case TaskStateComplete:
	}
	// entering new state
	task.State = state
	switch state {
	case TaskStateWaiting:
	case TaskStateReady:
		runner.enqueuedTasks[name] = true
		runner.readyTasks <- name
	case TaskStateInProgress:
		runner.inprogressTasks[name] = true
	case TaskStateFailed:
	case TaskStateSkipped:
	case TaskStateComplete:
	}
}
