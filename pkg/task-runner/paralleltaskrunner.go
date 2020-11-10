package task_runner

import (
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type RunnerTask struct {
	State    TaskState
	Task     Task
	Id       string
	Duration time.Duration
	// UpstreamDeps: what unfinished tasks do I depend on?
	// When the number of UpstreamDeps goes to 0, the Task is ready to run and is added to the `Ready` queue.
	UpstreamDeps map[string]bool
	// DownstreamDeps: what tasks are waiting for me to finish?
	DownstreamDeps map[string]bool
}

type DidUpdateTaskState func(Task, TaskState)

type ParallelTaskRunnerState string

const (
	ParallelTaskRunnerStatePrepared ParallelTaskRunnerState = "ParallelTaskRunnerStatePrepared"
	ParallelTaskRunnerStateRunning  ParallelTaskRunnerState = "ParallelTaskRunnerStateRunning"
	ParallelTaskRunnerStateStopped  ParallelTaskRunnerState = "ParallelTaskRunnerStateStopped"
)

type ParallelTaskRunner struct {
	State              ParallelTaskRunnerState
	Tasks              map[string]*RunnerTask
	Concurrency        int
	didUpdateTaskState DidUpdateTaskState
	stopChan           chan struct{}
	actions            chan func()
	readyTasks         chan string
}

func NewDefaultParallelTaskRunner(concurrency int) *ParallelTaskRunner {
	return NewParallelTaskRunner(concurrency, nil)
}

func NewParallelTaskRunner(concurrency int, didUpdateTaskState DidUpdateTaskState) *ParallelTaskRunner {
	return NewParallelTaskRunnerWithQueueSize(concurrency, didUpdateTaskState, 1000)
}

func NewParallelTaskRunnerWithQueueSize(concurrency int, didUpdateTaskState DidUpdateTaskState, queueSize int) *ParallelTaskRunner {
	runner := &ParallelTaskRunner{
		State:              ParallelTaskRunnerStatePrepared,
		Tasks:              map[string]*RunnerTask{},
		Concurrency:        concurrency,
		didUpdateTaskState: didUpdateTaskState,
		stopChan:           make(chan struct{}),
		actions:            make(chan func()),
		readyTasks:         make(chan string, queueSize),
	}
	go func() {
		for {
			var action func()
			select {
			case <-runner.stopChan:
				return
			case action = <-runner.actions:
			}

			// TODO metrics, logging, error reporting
			action()
		}
	}()
	return runner
}

func (runner *ParallelTaskRunner) createWorker() {
	go func() {
		for {
			var taskName string
			select {
			case <-runner.stopChan:
				return
			case taskName = <-runner.readyTasks:
			}

			runner.runTask(taskName)
		}
	}()
}

// 'actions' are serialized on a single goroutine, and should be the only things allowed to modify
// internal state

func (runner *ParallelTaskRunner) setTaskStateAction(taskName string, state TaskState) {
	runner.actions <- func() {
		taskState, ok := runner.Tasks[taskName]
		if !ok {
			panic(errors.Errorf("unable to find task %s", taskName))
		}
		if state == taskState.State {
			// TODO also check for other disallowed transitions, i.e. Complete -> InProgress
			panic(errors.Errorf("task %s already in state %s", taskName, state.String()))
		}
		taskState.State = state

		if runner.didUpdateTaskState != nil {
			go runner.didUpdateTaskState(taskState.Task, taskState.State)
		}
	}
}

func (runner *ParallelTaskRunner) getTaskStateAction(taskName string) TaskState {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	var state TaskState
	runner.actions <- func() {
		runnerTask, ok := runner.Tasks[taskName]
		if !ok {
			panic(errors.Errorf("unable to find task %s", taskName))
		}
		state = runnerTask.State
		wg.Done()
	}
	wg.Wait()
	return state
}

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

		runnerTask.State = TaskStateInProgress
		task = runnerTask.Task
		wg.Done()

		if runner.didUpdateTaskState != nil {
			go runner.didUpdateTaskState(runnerTask.Task, runnerTask.State)
		}
	}
	wg.Wait()
	return task
}

func (runner *ParallelTaskRunner) didFinishTaskAction(taskName string, state TaskState, err error, duration time.Duration) {
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
		runnerTask.State = state
		runnerTask.Duration = duration
		for downstreamName := range runnerTask.DownstreamDeps {
			downstream := runner.Tasks[downstreamName]
			delete(downstream.UpstreamDeps, taskName)
			if len(downstream.UpstreamDeps) == 0 {
				if downstream.State != TaskStateWaiting {
					panic(errors.Errorf("expected state Waiting for task %s, found %s", downstreamName, downstream.State.String()))
				}
				downstream.State = TaskStateReady
				runner.readyTasks <- downstreamName
			}
		}
		wg.Done()

		if runner.didUpdateTaskState != nil {
			go runner.didUpdateTaskState(runnerTask.Task, runnerTask.State)
		}
	}
	wg.Wait()
}

func (runner *ParallelTaskRunner) addNewTasksAction(tasks map[string]*RunnerTask) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	runner.actions <- func() {
		for name, runnerTask := range tasks {
			runner.Tasks[name] = runnerTask
			// no upstream dependencies?  ready to execute, queue it up!
			if len(runnerTask.UpstreamDeps) == 0 {
				runnerTask.State = TaskStateReady
				runner.readyTasks <- name

				if runner.didUpdateTaskState != nil {
					go runner.didUpdateTaskState(runnerTask.Task, runnerTask.State)
				}
			}
		}
		wg.Done()
	}
	wg.Wait()
}

func (runner *ParallelTaskRunner) getTaskIdAction(taskName string) *string {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	var id *string
	runner.actions <- func() {
		runnerTask, ok := runner.Tasks[taskName]
		if ok {
			*id = runnerTask.Id
		}
		wg.Done()
	}
	wg.Wait()
	return id
}

func (runner *ParallelTaskRunner) getStateAction() ParallelTaskRunnerState {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	var state ParallelTaskRunnerState
	runner.actions <- func() {
		state = runner.State
		wg.Done()
	}
	wg.Wait()
	return state
}

func (runner *ParallelTaskRunner) startAction() error {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	var err error
	runner.actions <- func() {
		if runner.State == ParallelTaskRunnerStatePrepared {
			runner.State = ParallelTaskRunnerStateRunning
			for i := 0; i < runner.Concurrency; i++ {
				runner.createWorker()
			}
		} else {
			err = errors.Errorf("expected state Prepared, found state %s", runner.State)
		}
		wg.Done()
	}
	wg.Wait()
	return err
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
	runner.didFinishTaskAction(taskName, state, err, time.Since(start))
}

func (runner *ParallelTaskRunner) AddTask(task Task) error {
	if runner.State == ParallelTaskRunnerStateStopped {
		return errors.Errorf("unable to add task: runner is stopped")
	}

	// 1. build the task tables
	//tasks, err := BuildDependencyTables(task)
	tasks, err := BuildDependencyTablesIterative(task)
	if err != nil {
		return err
	}
	// 2. validate -- check that there's no duplicates between existing and new stuff
	for name, runnerTask := range tasks {
		if prevId := runner.getTaskIdAction(name); prevId != nil {
			if *prevId != runnerTask.Id {
				return errors.Errorf("can't add task of name %s and id %s, already present with id %s", name, runnerTask.Id, *prevId)
			}
			// otherwise, it's the same Task -- and that's okay: we're adding a new task
			// with a dependency on an old task
		}
	}
	// 3. if everything checks out -- then add the new stuff
	runner.addNewTasksAction(tasks)

	return nil
}

func (runner *ParallelTaskRunner) Start() error {
	return runner.startAction()
}

func (runner *ParallelTaskRunner) Stop() error {
	return runner.stopAction()
}
