package examples

import (
	tr "github.com/mattfenwick/task-runner/pkg/task-runner"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func node(name string, deps ...tr.Task) *tr.FunctionTask {
	return tr.NewNoopTask(name, deps)
}

var (
	nd = node("d")
	nc = node("c", nd)
	nb = node("b", nd)
	na = node("a", nb, nc)
)

func RunTraversalsTests() {
	Describe("Traversals", func() {
		It("Should visit each node once and only once", func() {
			counts := map[string]int{}
			tr.TaskTraverse(na, func(currentTask tr.Task, depth int) {
				counts[currentTask.TaskName()]++
			})
			for _, name := range []string{"a", "b", "c", "d"} {
				Expect(counts).To(HaveKeyWithValue(name, 1))
			}
		})
	})
}
