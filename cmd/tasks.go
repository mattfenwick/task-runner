package main

import (
	. "github.com/mattfenwick/task-runner/pkg"
	"log"
	"os/exec"
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

	err := tr.TaskRunnerRun(a)
	doOrDie(err)

	cmd := exec.Command("ls", "-al")
	err = RunCommandAndPrint(cmd)
	doOrDie(err)
}
