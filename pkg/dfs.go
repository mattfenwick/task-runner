package pkg

import (
	"encoding/json"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type DFSNodeState string

const (
	DFSNodeStateReady      DFSNodeState = "DFSNodeStateReady"
	DFSNodeStateInProgress DFSNodeState = "DFSNodeStateInProgress"
	DFSNodeStateDone       DFSNodeState = "DFSNodeStateDone"
)

type DFSNode struct {
	RunnerTask *RunnerTask
	State      DFSNodeState
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
		oldId, newId := node.RunnerTask.Id, ObjectId(currentTask)
		if oldId != newId {
			return nil, errors.Errorf("duplicate task name '%s' detected, ids %s and %s", name, oldId, newId)
		}
	} else {
		dfsNodes[name] = &DFSNode{
			RunnerTask: &RunnerTask{
				State:          TaskStateWaiting,
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

func BuildDependencyTablesIterative(rootTask Task) (map[string]*RunnerTask, error) {
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

		for _, dep := range node.RunnerTask.Task.TaskDependencies() {
			depName := dep.TaskName()
			dfsNodes[nextName].RunnerTask.UpstreamDeps[depName] = true
			// TODO potential optimization: if node is already done, don't re-push it
			stack, err = enqueue(stack, dfsNodes, dep)
			if err != nil {
				return nil, err
			}
			dfsNodes[depName].RunnerTask.DownstreamDeps[nextName] = true
		}
	}

	tasks := map[string]*RunnerTask{}
	for taskName, dfsNode := range dfsNodes {
		tasks[taskName] = dfsNode.RunnerTask
	}
	return tasks, nil
}

// TODO:
//   - cycle detection
//   - duplicate detection (done?)
//   - gather up prereqs
//   make sure to differentiate between two different tasks having a dep on something, and two different tasks having the same name
func BuildDependencyTables(task Task) (map[string]*RunnerTask, error) {
	tasks := map[string]*RunnerTask{}
	err := buildDependencyTablesHelp(task, tasks)
	return tasks, err
}

func buildDependencyTablesHelp(task Task, tasks map[string]*RunnerTask) error {
	id := ObjectId(task)
	name := task.TaskName()
	if prev, ok := tasks[name]; ok {
		if prev.Id == id {
			return nil
		}
		return errors.Errorf("duplicate task %s, ids %s and %s", name, id, prev.Id)
	}
	tasks[name] = &RunnerTask{
		State:          TaskStateWaiting,
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
