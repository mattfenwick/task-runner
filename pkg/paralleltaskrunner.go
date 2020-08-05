package pkg

import (
	"encoding/json"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"sync"
)

type ParallelTaskRunnerTaskState string

// TODO could also add:
//   canceled by caller or something
//   parse out different kinds of failures (run, isdone, prereq, dep)
const (
	ParallelTaskRunnerTaskStateReady      ParallelTaskRunnerTaskState = "ParallelTaskRunnerTaskStateReady"
	ParallelTaskRunnerTaskStateWaiting    ParallelTaskRunnerTaskState = "ParallelTaskRunnerTaskStateWaiting"
	ParallelTaskRunnerTaskStateInProgress ParallelTaskRunnerTaskState = "ParallelTaskRunnerTaskStateInProgress"
	ParallelTaskRunnerTaskStateFailed     ParallelTaskRunnerTaskState = "ParallelTaskRunnerTaskStateFailed"
	ParallelTaskRunnerTaskStateSkipped    ParallelTaskRunnerTaskState = "ParallelTaskRunnerTaskStateSkipped"
	ParallelTaskRunnerTaskStateComplete   ParallelTaskRunnerTaskState = "ParallelTaskRunnerTaskStateComplete"
)

func (s ParallelTaskRunnerTaskState) String() string {
	switch s {
	case ParallelTaskRunnerTaskStateReady:
		return "Ready"
	case ParallelTaskRunnerTaskStateWaiting:
		return "Waiting"
	case ParallelTaskRunnerTaskStateInProgress:
		return "InProgress"
	case ParallelTaskRunnerTaskStateFailed:
		return "Failed"
	case ParallelTaskRunnerTaskStateSkipped:
		return "Skipped"
	case ParallelTaskRunnerTaskStateComplete:
		return "Complete"
	default:
		panic(errors.Errorf("invalid ParallelTaskRunnerTaskState value %s", string(s)))
	}
}

type TaskState struct {
	State ParallelTaskRunnerTaskState
	Task  Task
	Id    string
	// UpstreamDeps: given a Task `X`, what unfinished tasks does `X` depend on?  This is used to determine
	//   whether a Task is ready to run, or is still waiting for its dependencies to finish.
	//   When the number of UpstreamDeps goes to 0, the Task is ready to run and is added to the
	//   `Ready` queue.
	// tl,dr; what do I depend on?
	UpstreamDeps map[string]bool
	// DownstreamDeps: given a Task `Y`, what tasks depend on `Y`?  This is used to determine which Task's
	//   entries in the UpstreamDeps table need to be updated upon completion of task `Y`.  For example, if
	//   A, B, and Z depend on Y, when Y finishes, the UpstreamDeps entries for A, B, Z will be updated to
	//   remove Y.
	// tl,dr; what depends on me?
	DownstreamDeps map[string]bool
}

type ParallelTaskRunner struct {
	// Tasks tracks the tasks and their states
	Tasks         map[string]*TaskState
	Concurrency   int
	didFinishTask func(Task, ParallelTaskRunnerTaskState, error)
	stopChan      chan struct{}
	actions       chan func()
	// readyTasks are Tasks which are ready to execute.  The next available worker will take the
	// next available Task.
	readyTasks chan string
}

// TODO would like to be able to add new tasks as execution happens
// TODO add a stop method?  have it stop itself after finishing all tasks -- or have it just sit there until stop is called?
func NewParallelTaskRunner(concurrency int, didFinishTask func(Task, ParallelTaskRunnerTaskState, error)) *ParallelTaskRunner {
	runner := &ParallelTaskRunner{
		Tasks:         map[string]*TaskState{},
		Concurrency:   concurrency,
		didFinishTask: didFinishTask,
		stopChan:      make(chan struct{}),
		actions:       make(chan func()),
		// TODO this is a super arbitrary number, is there a better way to set this?  goal is to avoid blocking writes
		//   to the channel, without letting it use too much memory
		readyTasks: make(chan string, 1000),
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

func (runner *ParallelTaskRunner) setTaskStateAction(taskName string, state ParallelTaskRunnerTaskState) {
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
	}
}

func (runner *ParallelTaskRunner) getTaskStateAction(taskName string) ParallelTaskRunnerTaskState {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	var state ParallelTaskRunnerTaskState
	runner.actions <- func() {
		taskState, ok := runner.Tasks[taskName]
		if !ok {
			panic(errors.Errorf("unable to find task %s", taskName))
		}
		state = taskState.State
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
		taskState := runner.Tasks[taskName]
		if taskState.State != ParallelTaskRunnerTaskStateReady {
			panic(errors.Errorf("task %s in invalid state, expected Ready, got %s", taskName, taskState.State.String()))
		}

		taskState.State = ParallelTaskRunnerTaskStateInProgress
		task = taskState.Task
		wg.Done()
	}
	wg.Wait()
	return task
}

func (runner *ParallelTaskRunner) markTaskCompleteAction(taskName string) {
	// Any tasks that depend on this one, now have one less dependency blocking their execution.
	// If anything gets down to 0, queue it up!
	wg := &sync.WaitGroup{}
	wg.Add(1)
	runner.actions <- func() {
		taskState := runner.Tasks[taskName]
		for downstreamName := range taskState.DownstreamDeps {
			downstream := runner.Tasks[downstreamName]
			delete(downstream.UpstreamDeps, taskName)
			if len(downstream.UpstreamDeps) == 0 {
				if downstream.State != ParallelTaskRunnerTaskStateWaiting {
					panic(errors.Errorf("expected state Waiting for task %s, found %s", downstreamName, downstream.State.String()))
				}
				downstream.State = ParallelTaskRunnerTaskStateReady
				runner.readyTasks <- downstreamName
			}
		}
		wg.Done()
	}
	wg.Wait()
}

func (runner *ParallelTaskRunner) addNewTasksAction(tasks map[string]*TaskState) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	runner.actions <- func() {
		for name, taskState := range tasks {
			runner.Tasks[name] = taskState
			// no upstream dependencies?  ready to execute, queue it up!
			if len(taskState.UpstreamDeps) == 0 {
				taskState.State = ParallelTaskRunnerTaskStateReady
				runner.readyTasks <- name
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
		taskState, ok := runner.Tasks[taskName]
		if ok {
			*id = taskState.Id
		}
		wg.Done()
	}
	wg.Wait()
	return id
}

// running a task

func (runner *ParallelTaskRunner) runTaskHelper(task Task) error {
	taskName := task.TaskName()

	// If a task is already done, then don't run it.
	isDone, err := task.TaskIsDone()
	if err != nil {
		runner.setTaskStateAction(taskName, ParallelTaskRunnerTaskStateFailed)
		return err
	}
	if isDone {
		log.Debugf("skipping task %s, already done", taskName)
		runner.setTaskStateAction(taskName, ParallelTaskRunnerTaskStateSkipped)
		return nil
	}

	// Make sure all the prerequisites for a task are met.
	for _, p := range task.TaskPrereqs() {
		log.Debugf("checking prereq %s of task %s", p.PrereqName(), taskName)
		if err := p.PrereqRun(); err != nil {
			runner.setTaskStateAction(taskName, ParallelTaskRunnerTaskStateFailed)
			return errors.WithMessagef(err, "prereq '%s' failed for task %s", p.PrereqName(), taskName)
		}
		log.Debugf("prereq %s of task %s is good to go", p.PrereqName(), taskName)
	}

	// Actually run the task.
	log.Debugf("running task %s", taskName)
	err = task.TaskRun()
	if err != nil {
		runner.setTaskStateAction(taskName, ParallelTaskRunnerTaskStateFailed)
		return errors.WithMessagef(err, "failed to run task %s", taskName)
	}
	log.Debugf("finished running task %s", taskName)

	// After running a task, make sure that it considers itself to be done.
	isDone, err = task.TaskIsDone()
	if err != nil {
		runner.setTaskStateAction(taskName, ParallelTaskRunnerTaskStateFailed)
		return errors.WithMessagef(err, "task %s failed post-execution IsDone check", taskName)
	}
	if !isDone {
		runner.setTaskStateAction(taskName, ParallelTaskRunnerTaskStateFailed)
		return errors.Errorf("ran task %s but it still reports itself as not done", taskName)
	}

	runner.setTaskStateAction(taskName, ParallelTaskRunnerTaskStateComplete)
	return nil
}

func (runner *ParallelTaskRunner) runTask(taskName string) {
	task := runner.startTaskAction(taskName)

	err := runner.runTaskHelper(task)
	state := runner.getTaskStateAction(taskName)
	if err != nil {
		// TODO anything else to do here?
		log.Errorf("failed to run task %s: %+v", taskName, err)
	} else {
		if !(state == ParallelTaskRunnerTaskStateSkipped || state == ParallelTaskRunnerTaskStateComplete) {
			panic(errors.Errorf("expected task state Skipped or Complete, found %s", state.String()))
		}
		runner.markTaskCompleteAction(taskName)
	}

	if runner.didFinishTask != nil {
		runner.didFinishTask(task, state, err)
	}
}

func (runner *ParallelTaskRunner) AddTask(task Task) error {
	// 1. build the task tables
	//tasks, err := BuildDependencyTables(task)
	tasks, err := BuildDependencyTablesIterative(task)
	if err != nil {
		return err
	}
	// 2. validate -- check that there's no duplicates between existing and new stuff
	for name, taskState := range tasks {
		if prevId := runner.getTaskIdAction(name); prevId != nil {
			if *prevId != taskState.Id {
				return errors.Errorf("can't add task of name %s and id %s, already present with id %s", name, taskState.Id, *prevId)
			}
			// otherwise, it's the same Task -- and that's okay: we're adding a new task
			// with a dependency on an old task
		}
	}
	// 3. if everything checks out -- then add the new stuff
	runner.addNewTasksAction(tasks)

	return nil
}

func (runner *ParallelTaskRunner) Run() error {
	// TODO check to make sure we 1) haven't started, and 2) haven't finished
	//   if stopChan is already closed, return an error?  or maybe we just don't need a return value?
	for i := 0; i < runner.Concurrency; i++ {
		runner.createWorker()
	}
	return nil
}

func (runner *ParallelTaskRunner) Stop() {
	close(runner.stopChan)
}

// these functions flatten the dependencies of tasks into two one-way tables

type DFSNodeState string

const (
	DFSNodeStateReady      DFSNodeState = "DFSNodeStateReady"
	DFSNodeStateInProgress DFSNodeState = "DFSNodeStateInProgress"
	DFSNodeStateDone       DFSNodeState = "DFSNodeStateDone"
)

type DFSNode struct {
	TaskState *TaskState
	State     DFSNodeState
}

type StackActionType string

const (
	StackActionTypePush StackActionType = "StackActionTypePush"
	StackActionTypePop  StackActionType = "StackActionTypePop"
)

type StackAction struct {
	Type StackActionType
	Name string
}

func enqueue(stack []*StackAction, dfsNodes map[string]*DFSNode, currentTask Task) ([]*StackAction, error) {
	name := currentTask.TaskName()
	stack = append(stack, &StackAction{Type: StackActionTypePush, Name: name})
	if node, ok := dfsNodes[name]; ok {
		oldId, newId := node.TaskState.Id, ObjectId(currentTask)
		if oldId != newId {
			return nil, errors.Errorf("duplicate task name '%s' detected, ids %s and %s", name, oldId, newId)
		}
	} else {
		dfsNodes[name] = &DFSNode{
			TaskState: &TaskState{
				State:          ParallelTaskRunnerTaskStateWaiting,
				Task:           currentTask,
				Id:             ObjectId(currentTask),
				UpstreamDeps:   map[string]bool{},
				DownstreamDeps: map[string]bool{},
			},
			State: DFSNodeStateReady,
		}
	}
	return stack, nil
}

func BuildDependencyTablesIterative(rootTask Task) (map[string]*TaskState, error) {
	var stack []*StackAction
	dfsNodes := map[string]*DFSNode{}

	// don't see how the first push could ever fail, so ignore the error
	stack, _ = enqueue(stack, dfsNodes, rootTask)

ForLoop:
	for {
		// Stack's empty?  Then we're done
		n := len(stack)
		if n == 0 {
			break ForLoop
		}

		stackBytes, err := json.Marshal(stack)
		if err != nil {
			panic(err)
		}
		log.Debugf("stack: %s", stackBytes)

		// Pop the stack and get the next task name
		top := stack[n-1]
		stack = stack[:n-1]
		nextName := top.Name
		if top.Type == StackActionTypePop {
			state := dfsNodes[top.Name].State
			if state != DFSNodeStateInProgress {
				panic(errors.Errorf("expected state InProgress for %s, found %s", nextName, state))
			}
			log.Debugf("finishing traversal of task %s", nextName)
			dfsNodes[nextName].State = DFSNodeStateDone
			continue ForLoop
		}

		log.Debugf("looking at %s", nextName)

		node := dfsNodes[nextName]
		switch node.State {
		case DFSNodeStateInProgress:
			// Task is already in progress => we have a cycle
			return nil, errors.Errorf("cycle detected -- stack %s, next %s", stackBytes, nextName)
		case DFSNodeStateDone:
			log.Debugf("task %s is already done: skipping", nextName)
			continue ForLoop
		case DFSNodeStateReady:
			// Task is ready => good to go, let's work on it
			log.Debugf("starting traversal work on task %s", nextName)
		default:
			panic(errors.Errorf("invalid node state %s", node.State))
		}

		node.State = DFSNodeStateInProgress
		// add a `Pop` action to the stack, so:
		//  1. we know what's in progress (used for cycle detection)
		//  2. we know when to mark something as finished
		stack = append(stack, &StackAction{Type: StackActionTypePop, Name: nextName})

		for _, dep := range node.TaskState.Task.TaskDependencies() {
			depName := dep.TaskName()
			dfsNodes[nextName].TaskState.UpstreamDeps[depName] = true
			// TODO potential optimization: if node is already done, don't re-push it
			stack, err = enqueue(stack, dfsNodes, dep)
			if err != nil {
				return nil, err
			}
			dfsNodes[depName].TaskState.DownstreamDeps[nextName] = true
		}
	}

	tasks := map[string]*TaskState{}
	for taskName, dfsNode := range dfsNodes {
		tasks[taskName] = dfsNode.TaskState
	}
	return tasks, nil
}

func BuildDependencyTables(task Task) (map[string]*TaskState, error) {
	tasks := map[string]*TaskState{}
	err := buildDependencyTablesHelp(task, tasks)
	return tasks, err
}

// TODO:
//   - cycle detection
//   - duplicate detection (done?)
//   - gather up prereqs
//   make sure to differentiate between two different tasks having a dep on something, and two different tasks having the same name
func buildDependencyTablesHelp(task Task, tasks map[string]*TaskState) error {
	id := ObjectId(task)
	name := task.TaskName()
	if prev, ok := tasks[name]; ok {
		if prev.Id == id {
			return nil
		}
		return errors.Errorf("duplicate task %s, ids %s and %s", name, id, prev.Id)
	}
	tasks[name] = &TaskState{
		State:          ParallelTaskRunnerTaskStateWaiting,
		Task:           task,
		Id:             id,
		UpstreamDeps:   map[string]bool{},
		DownstreamDeps: map[string]bool{},
	}
	for _, dep := range task.TaskDependencies() {
		depName := dep.TaskName()
		tasks[name].UpstreamDeps[depName] = true
		err := buildDependencyTablesHelp(dep, tasks)
		if err != nil {
			return err
		}
		tasks[depName].DownstreamDeps[name] = true
	}
	return nil
}
