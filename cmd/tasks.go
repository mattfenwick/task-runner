package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mattfenwick/task-runner/pkg/examples"
	. "github.com/mattfenwick/task-runner/pkg/task-runner"
	log "github.com/sirupsen/logrus"
	"os/exec"
	"strings"
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

	results, err := tr.TaskRunnerRun(a, false)
	doOrDie(err)

	TaskTraverse(a, func(currentTask Task, level int) {
		annotation := "unknown"
		if result, ok := results[currentTask.TaskName()]; ok {
			annotation = string(result.State)
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
	runner, err := NewDefaultParallelTaskRunner(a, 5)
	doOrDie(err)
	taskResults := runner.Wait(context.TODO())

	log.Infof("task results: %+v", taskResults)

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

	fmt.Printf("dot graph before:\n%s\n", TaskToGraphDump(a).RenderAsDot(true))

	fmt.Printf("list before:\n%s\n", TaskToGraphDump(a).RenderAsList(true))

	TaskDebugPrint(a)

	tr := SimpleTaskRunner{}

	results, err := tr.TaskRunnerRun(a, false)
	doOrDie(err)

	TaskTraverse(a, func(currentTask Task, level int) {
		annotation := "unknown"
		if result, ok := results[currentTask.TaskName()]; ok {
			annotation = result.State.String()
		}
		fmt.Printf("%s%s: %s\n", strings.Repeat(" ", level*2), currentTask.TaskName(), annotation)
	})

	fmt.Printf("dot graph after:\n%s\n", TaskToGraphDump(a).RenderAsDot(true))

	fmt.Printf("list after:\n%s\n", TaskToGraphDump(a).RenderAsList(true))

	jsonDump(results)
}

func jsonDump(obj interface{}) {
	bytes, err := json.MarshalIndent(obj, "", "  ")
	doOrDie(err)
	fmt.Printf("%s\n\n", string(bytes))
}
