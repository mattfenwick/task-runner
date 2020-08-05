package main

import (
	"fmt"
	. "github.com/mattfenwick/task-runner/pkg"
	log "github.com/sirupsen/logrus"
	"os/exec"
	"strings"
	"sync"
)

func doOrDie(err error) {
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func taskGraph() Task {
	e := PrintTask("e")
	c := PrintTask("c",
		//PrintTask("b"),
		e,
	)
	a := PrintTask("a",
		PrintTask("b",
			c,
			PrintTask("d"),
			//PrintTask("d"),
			e,
		),
	)
	return a
}

func main() {
	a := taskGraph()
	TaskDebugPrint(a)

	tr := SimpleTaskRunner{}

	statuses, err := tr.TaskRunnerRun(a, false)
	doOrDie(err)

	TaskTraverse(a, func(currentTask Task, level int) {
		annotation := "unknown"
		if anno, ok := statuses[currentTask.TaskName()]; ok {
			annotation = string(anno)
		}
		fmt.Printf("%s: %s - %s\n", currentTask.TaskName(), strings.Repeat(" ", level*2), annotation)
	})

	idempotentExample()
	parallelExample()

	cmd := exec.Command("ls", "-al")
	err = RunCommandAndPrint(cmd)
	doOrDie(err)
}

func parallelExample() {
	fmt.Printf("\n\nparallel example:\n")
	a := taskGraph()
	wg := &sync.WaitGroup{}
	wg.Add(5)
	runner := NewParallelTaskRunner(5, func(task Task, state TaskState, err error) {
		wg.Done()
	})
	doOrDie(runner.AddTask(a))
	doOrDie(runner.Start())
	wg.Wait()

	TaskDebugPrint(a)

	_, task := setKeyTwiceGraph()
	taskStates, err := BuildDependencyTablesIterative(task)
	doOrDie(err)
	log.Infof("task states: %+v", taskStates)

	cycle := trivialCycle()
	taskStates, err = BuildDependencyTablesIterative(cycle)
	if err == nil {
		panic("expected error, found none")
	}
	log.Infof("trivial cycle error: %+v", err)
}

func idempotentExample() {
	fmt.Printf("\n\nidempotent example:\n")
	dict := map[string]bool{
		"c": true,
		"d": true,
	}
	e := SetKeyTaskIdempotent("e", "e", dict)
	c := SetKeyTaskIdempotent("c", "c", dict, e)
	a := SetKeyTaskIdempotent("a", "a", dict,
		SetKeyTaskIdempotent("b", "b", dict,
			c,
			SetKeyTaskIdempotent("d", "d", dict),
			e,
		),
	)

	TaskDebugPrint(a)

	tr := SimpleTaskRunner{}

	statuses, err := tr.TaskRunnerRun(a, false)
	doOrDie(err)

	TaskTraverse(a, func(currentTask Task, level int) {
		annotation := "unknown"
		if anno, ok := statuses[currentTask.TaskName()]; ok {
			annotation = anno.String()
		}
		fmt.Printf("%s%s: %s\n", strings.Repeat(" ", level*2), currentTask.TaskName(), annotation)
	})
}

func setKeyTwiceGraph() (map[string]bool, Task) {
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

func trivialCycle() Task {
	a := PrintTask("a")
	a.TaskAddDependency(a)
	return a
}
