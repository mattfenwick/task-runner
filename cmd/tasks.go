package main

import (
	"fmt"
	"github.com/mattfenwick/task-runner/pkg/examples"
	. "github.com/mattfenwick/task-runner/pkg/task-runner"
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
	e := examples.PrintTask("e")
	c := examples.PrintTask("c",
		//PrintTask("b"),
		e,
	)
	a := examples.PrintTask("a",
		examples.PrintTask("b",
			c,
			examples.PrintTask("d"),
			//PrintTask("d"),
			e,
		),
	)
	return a
}

func main() {
	//basicExample()
	idempotentExample()
	//parallelExample()
	//runAndPrintExample()
}

func basicExample() {
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
}

func runAndPrintExample() {
	cmd := exec.Command("ls", "-al")
	err := RunCommandAndPrint(cmd)
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

	_, task := examples.SetKeyTwiceGraph()
	taskStates, err := BuildDependencyTablesIterative(task)
	doOrDie(err)
	log.Infof("task states: %+v", taskStates)

	cycle := examples.TrivialCycleGraph()
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
	e := examples.SetKeyTaskIdempotent("e", "e", dict)
	c := examples.SetKeyTaskIdempotent("c", "c", dict, e)
	a := examples.SetKeyTaskIdempotent("a", "a", dict,
		examples.SetKeyTaskIdempotent("b", "b", dict,
			c,
			examples.SetKeyTaskIdempotent("d", "d", dict),
			e,
		),
	)

	path := "example-graph.png"
	err := TaskToGraphDump(a).ToDot(path, true)
	doOrDie(err)
	_, err = RunCommand(exec.Command("open", path))
	doOrDie(err)

	fmt.Printf("dot graph before:\n%s\n", TaskToGraphDump(a).RenderAsDot(true))

	fmt.Printf("list before:\n%s\n", TaskToGraphDump(a).RenderAsList(true))

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

	fmt.Printf("dot graph after:\n%s\n", TaskToGraphDump(a).RenderAsDot(true))

	fmt.Printf("list after:\n%s\n", TaskToGraphDump(a).RenderAsList(true))
}
