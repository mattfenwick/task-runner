package examples

import (
	task_runner "github.com/mattfenwick/task-runner/pkg/task-runner"
	"sync"
)

func ManyDepsOnATaskGraph() task_runner.Task {
	dAgain := PrintTask("d-again",
		PrintTask("d"))
	a := PrintTask("a",
		dAgain,
		PrintTask("b", dAgain),
		PrintTask("c", dAgain))
	return a
}

func TrivialCycleGraph() task_runner.Task {
	a := PrintTask("a")
	a.TaskAddDependency(a)
	return a
}

func NonTrivialCycleGraph() task_runner.Task {
	d := PrintTask("d")
	c := PrintTask("c", d)
	b := PrintTask("b", c)
	a := PrintTask("a", b)
	d.TaskAddDependency(a)
	return a
}

func SetKeyOnceGraph() (map[string]bool, task_runner.Task) {
	ran := map[string]bool{}
	mux := &sync.Mutex{}
	/*
		a -> b -> d -> e
		a -> c
		a -> d
	*/
	d := SetKeyTask("d", "d", mux, ran,
		SetKeyTask("e", "e", mux, ran))
	a := SetKeyTask("a", "a", mux, ran,
		SetKeyTask("b", "b", mux, ran, d),
		SetKeyTask("c", "c", mux, ran),
		d)
	return ran, a
}

func SetKeyCountGraph() (map[string]bool, map[string]*RunCountTask, task_runner.Task) {
	ran := map[string]bool{}
	d := SetKeyCountTask("d", "d", ran)
	dAgain := SetKeyCountTask("d-again", "d", ran, d)
	b := SetKeyCountTask("b", "b", ran, d)
	c := SetKeyCountTask("c", "c", ran, dAgain)
	a := SetKeyCountTask("a", "a", ran,
		b,
		c,
		d)
	tasks := map[string]*RunCountTask{
		"a":       a,
		"b":       b,
		"c":       c,
		"d":       d,
		"d-again": dAgain,
	}
	return ran, tasks, a
}

func SetKeyTwiceGraph() (map[string]bool, task_runner.Task) {
	ran := map[string]bool{}
	mux := &sync.Mutex{}
	dAgain := SetKeyTask("d-again", "d", mux, ran,
		SetKeyTask("d", "d", mux, ran))
	a := SetKeyTask("a", "a", mux, ran,
		SetKeyTask("b", "b", mux, ran, dAgain),
		SetKeyTask("c", "c", mux, ran, dAgain),
		dAgain)
	return ran, a
}

func SetKeyTwicePrereqGraph() (map[string]bool, task_runner.Task) {
	ran := map[string]bool{}
	dAgain := SetKeyTaskPrereq("d-again", "d", ran,
		SetKeyTaskPrereq("d", "d", ran))
	a := SetKeyTaskPrereq("a", "a", ran,
		SetKeyTaskPrereq("b", "b", ran, dAgain),
		SetKeyTaskPrereq("c", "c", ran, dAgain),
		dAgain)
	return ran, a
}
