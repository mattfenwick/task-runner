package main

import (
	"fmt"
	. "github.com/mattfenwick/task-runner/pkg"
	"log"
	"os/exec"
	"strings"
)

func doOrDie(err error) {
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func main() {
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

	cmd := exec.Command("ls", "-al")
	err = RunCommandAndPrint(cmd)
	doOrDie(err)
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
