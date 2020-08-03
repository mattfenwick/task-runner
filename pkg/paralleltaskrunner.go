package pkg

import (
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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
	// TODO use this to track what directly depends on each task
	//   whenever a task `X` is finished, all of the tasks that depend on `X` get `X` removed from their list
	//   if any of those end up with 0 unfulfilled dependencies, then they get added to 'Ready' queue
	//   if `X` fails, all of the tasks depending on `X` move to FailedDependency and can not run
	// UpstreamDeps: given a Task `X`, what unfinished tasks does `X` depend on?  This is used to determine
	//   whether a Task is ready to run, or is still waiting for its dependencies to finish.
	//   When the number of UpstreamDeps goes to 0, the Task is ready to run and is added to the
	//   `Ready` queue.
	// TODO: clarify whether Once `X` is started, it should be removed from UpstreamDeps
	UpstreamDeps map[string]bool
	// DownstreamDeps: given a Task `Y`, what tasks depend on `Y`?  This is used to determine which Task's
	//   entries in the UpstreamDeps table need to be updated upon completion of task `Y`.  For example, if
	//   A, B, and Z depend on Y, when Y finishes, the UpstreamDeps entries for A, B, Z will be updated to
	//   remove Y.
	// TODO: clarify whether once Y is finished, it should be removed from DownstreamDeps
	DownstreamDeps map[string]bool
}

type ParallelTaskRunner struct {
	// readyTasks are Tasks which are ready to execute.  The next available worker will take the
	// next available Task.
	readyTasks chan string
	// Tasks tracks the tasks and their states
	Tasks         map[string]*TaskState
	Concurrency   int
	didFinishTask func(Task, ParallelTaskRunnerTaskState, error)
	stopChan      chan struct{}
}

// TODO would like to be able to add new tasks as execution happens
// TODO add a stop method?  have it stop itself after finishing all tasks -- or have it just sit there until stop is called?
func NewParallelTaskRunner(concurrency int, didFinishTask func(Task, ParallelTaskRunnerTaskState, error)) *ParallelTaskRunner {
	runner := &ParallelTaskRunner{
		// TODO this is a super arbitrary number, is there a better way to set this?  goal is to avoid blocking writes
		//   to the channel, without letting it use too much memory
		readyTasks:    make(chan string, 1000),
		Tasks:         map[string]*TaskState{},
		Concurrency:   concurrency,
		didFinishTask: didFinishTask,
		stopChan:      make(chan struct{}),
	}
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

func (runner *ParallelTaskRunner) runTaskHelper(taskState *TaskState) error {
	task := taskState.Task
	// If a task is already done, then don't run it.
	isDone, err := task.TaskIsDone()
	if err != nil {
		taskState.State = ParallelTaskRunnerTaskStateFailed
		return err
	}
	if isDone {
		log.Infof("skipping task %s, already done", task.TaskName())
		taskState.State = ParallelTaskRunnerTaskStateSkipped
		return nil
	}

	// Make sure all the prerequisites for a task are met.
	for _, p := range task.TaskPrereqs() {
		log.Infof("checking prereq %s of task %s", p.PrereqName(), task.TaskName())
		if err := p.PrereqRun(); err != nil {
			taskState.State = ParallelTaskRunnerTaskStateFailed
			return errors.WithMessagef(err, "prereq '%s' failed for task %s", p.PrereqName(), task.TaskName())
		}
		log.Infof("prereq %s of task %s is good to go", p.PrereqName(), task.TaskName())
	}

	// Actually run the task.
	log.Infof("running task %s", task.TaskName())
	err = task.TaskRun()
	if err != nil {
		taskState.State = ParallelTaskRunnerTaskStateFailed
		return errors.WithMessagef(err, "failed to run task %s", task.TaskName())
	}
	log.Infof("finished running task %s", task.TaskName())

	// After running a task, make sure that it considers itself to be done.
	isDone, err = task.TaskIsDone()
	if err != nil {
		taskState.State = ParallelTaskRunnerTaskStateFailed
		return errors.WithMessagef(err, "task %s failed post-execution IsDone check", task.TaskName())
	}
	if !isDone {
		taskState.State = ParallelTaskRunnerTaskStateFailed
		return errors.Errorf("ran task %s but it still reports itself as not done", task.TaskName())
	}

	taskState.State = ParallelTaskRunnerTaskStateComplete
	return nil
}

func (runner *ParallelTaskRunner) runTask(taskName string) {
	taskState := runner.Tasks[taskName]
	if taskState.State != ParallelTaskRunnerTaskStateReady {
		panic(errors.Errorf("task %s in invalid state, expected Ready, got %s", taskName, taskState.State.String()))
	}

	taskState.State = ParallelTaskRunnerTaskStateInProgress
	err := runner.runTaskHelper(taskState)
	if err != nil {
		// TODO anything else I should do here?
		log.Errorf("failed to run task %s: %+v", taskName, err)
	} else {
		// TODO sanity check: task state is now either Complete or Skipped
		// Any tasks that depend on this one, now have one less dependency blocking their execution.
		// If anything gets down to 0, queue it up!
		for downstream := range taskState.DownstreamDeps {
			delete(runner.Tasks[downstream].UpstreamDeps, taskName)
			if len(runner.Tasks[downstream].UpstreamDeps) == 0 {
				runner.Tasks[downstream].State = ParallelTaskRunnerTaskStateReady
				runner.readyTasks <- downstream
			}
		}
	}

	if runner.didFinishTask != nil {
		runner.didFinishTask(taskState.Task, taskState.State, err)
	}
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
		State:          ParallelTaskRunnerTaskStateWaiting, // TODO is this the right state?
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

func BuildDependencyTables(task Task) (tasks map[string]*TaskState, err error) {
	tasks = map[string]*TaskState{}
	err = buildDependencyTablesHelp(task, tasks)
	return
}

func (runner *ParallelTaskRunner) AddTask(task Task) error {
	// 1. build the task tables
	tasks, err := BuildDependencyTables(task)
	if err != nil {
		return err
	}
	// 2. validate -- check that there's no duplicates between existing and new stuff
	for name, taskState := range tasks {
		if prev, ok := runner.Tasks[name]; ok {
			if prev.Id != taskState.Id {
				return errors.Errorf("can't add task of name %s, already present", name)
			}
			// otherwise, it's the same Task -- and that's okay: we're adding a new task
			// with a dependency on an old task
		}
	}
	// 3. if everything checks out -- then add the new stuff
	for name, taskState := range tasks {
		runner.Tasks[name] = taskState
		// 4. no upstream dependencies?  ready to execute, queue it up!
		if len(taskState.UpstreamDeps) == 0 {
			taskState.State = ParallelTaskRunnerTaskStateReady
			runner.readyTasks <- name
		}
	}

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

// this method is useless
func (runner *ParallelTaskRunner) TaskRunnerRun(task Task) error {
	runner.AddTask(task)
	return runner.Run()
}
