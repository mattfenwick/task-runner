package pkg

import (
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"os/exec"
	"strings"
)

type Task interface {
	TaskName() string
	TaskDependencies() []Task
	TaskPrereqs() []Prereq
	TaskRun() error
	TaskIsDone() (bool, error)

	// TODO should this add error-checking for:
	//   adding a dependency that's already there (probably yes)
	//   adding a dependency to itself (not sure)
	TaskAddDependency(dep Task)
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

func traverseHelp(currentTask Task, depth int, f func(Task, int)) {
	f(currentTask, depth)
	for _, dep := range currentTask.TaskDependencies() {
		traverseHelp(dep, depth+1, f)
	}
}

// TaskTraverse executes a preorder DFS of the task graph.
//   TODO add support for dupe/cycle detection?
func TaskTraverse(t Task, f func(Task, int)) {
	traverseHelp(t, 0, f)
}

func TaskDebugPrint(rootTask Task) {
	TaskTraverse(rootTask, func(currentTask Task, level int) {
		fmt.Printf("%s%s: %p, %p\n", strings.Repeat(" ", level*2), currentTask.TaskName(), currentTask, &currentTask)
	})
}

func ObjectId(o interface{}) string {
	return fmt.Sprintf("%p", o)
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

type FunctionTask struct {
	Name    string
	Run     func() error
	Deps    []Task
	Prereqs []Prereq
	IsDone  func() (bool, error)
}

func NewFunctionTask(name string, run func() error, deps []Task, prereqs []Prereq, isDone func() (bool, error)) *FunctionTask {
	return &FunctionTask{
		Name:    name,
		Run:     run,
		Deps:    deps,
		Prereqs: prereqs,
		IsDone:  isDone,
	}
}

func (ft *FunctionTask) TaskName() string {
	return ft.Name
}

func (ft *FunctionTask) TaskDependencies() []Task {
	return ft.Deps
}

func (ft *FunctionTask) TaskPrereqs() []Prereq {
	return ft.Prereqs
}

func (ft *FunctionTask) TaskRun() error {
	return ft.Run()
}

func (ft *FunctionTask) TaskIsDone() (bool, error) {
	return ft.IsDone()
}

func (ft *FunctionTask) TaskAddDependency(dep Task) {
	ft.Deps = append(ft.Deps, dep)
}

func NewShellTask(name string, cmd *exec.Cmd, deps []Task, prereqs []Prereq, isDone func() (bool, error)) *FunctionTask {
	run := func() error {
		return RunCommandAndPrint(cmd)
	}
	return NewFunctionTask(name, run, deps, prereqs, isDone)
}
