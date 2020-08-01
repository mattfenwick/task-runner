package pkg

import "fmt"

// PrintTask prints a string, and marks itself as done after execution.
func PrintTask(s string, deps ...Task) Task {
	isDone := false
	return NewFunctionTask(s, func() error {
		fmt.Println(s)
		isDone = true
		return nil
	}, deps, []Prereq{}, func() (b bool, err error) {
		return isDone, nil
	})
}
