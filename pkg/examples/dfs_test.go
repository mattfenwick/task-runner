package examples

import (
	"encoding/json"
	"fmt"
	"github.com/mattfenwick/task-runner/pkg/task-runner"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func RunDfsTests() {
	Describe("BuildDependencyTables", func() {
		Describe("Iterative", func() {
			It("Handles a DAG where a node appears multiple times", func() {
				_, task := SetKeyTwiceGraph()
				taskStates, err := task_runner.BuildDependencyTablesIterative(task)
				Expect(err).To(Succeed())

				Expect(taskStates).To(HaveLen(5))
			})

			It("Handles a DAG where a node is in the stack multiple times at once", func() {
				task := ManyDepsOnATaskGraph()
				taskStates, err := task_runner.BuildDependencyTablesIterative(task)
				Expect(err).To(Succeed())

				Expect(taskStates).To(HaveLen(5))

				bs, _ := json.MarshalIndent(taskStates, "", "  ")
				fmt.Printf("\n%s\n", bs)

				Expect(taskStates["a"].UpstreamDeps).To(HaveKeyWithValue("d-again", true))
				Expect(taskStates["a"].UpstreamDeps).To(HaveKeyWithValue("b", true))
				Expect(taskStates["a"].UpstreamDeps).To(HaveKeyWithValue("c", true))
				Expect(taskStates["a"].DownstreamDeps).To(BeEmpty())

				Expect(taskStates["b"].UpstreamDeps).To(HaveKeyWithValue("d-again", true))
				Expect(taskStates["b"].DownstreamDeps).To(HaveKeyWithValue("a", true))

				Expect(taskStates["c"].UpstreamDeps).To(HaveKeyWithValue("d-again", true))
				Expect(taskStates["c"].DownstreamDeps).To(HaveKeyWithValue("a", true))

				Expect(taskStates["d-again"].UpstreamDeps).To(HaveKeyWithValue("d", true))
				Expect(taskStates["d-again"].DownstreamDeps).To(HaveKeyWithValue("a", true))
				Expect(taskStates["d-again"].DownstreamDeps).To(HaveKeyWithValue("b", true))
				Expect(taskStates["d-again"].DownstreamDeps).To(HaveKeyWithValue("c", true))

				Expect(taskStates["d"].UpstreamDeps).To(BeEmpty())
				Expect(taskStates["d"].DownstreamDeps).To(HaveKeyWithValue("d-again", true))
			})

			It("Recognizes a trivial cycle", func() {
				task := TrivialCycleGraph()
				taskStates, err := task_runner.BuildDependencyTablesIterative(task)
				Expect(err).NotTo(Succeed())
				Expect(err.Error()).To(MatchRegexp("cycle detected"))

				Expect(taskStates).To(HaveLen(0))
			})

			It("Recognizes a non-trivial cycle", func() {
				task := NonTrivialCycleGraph()
				taskStates, err := task_runner.BuildDependencyTablesIterative(task)
				Expect(err).NotTo(Succeed())
				Expect(err.Error()).To(MatchRegexp("cycle detected"))

				Expect(taskStates).To(HaveLen(0))
			})

			dupeNamesGraph := func() task_runner.Task {
				a2 := PrintTask("a")
				a := PrintTask("a", a2)
				return a
			}

			It("Recognizes duplicate names", func() {
				task := dupeNamesGraph()
				taskStates, err := task_runner.BuildDependencyTablesIterative(task)
				Expect(err).NotTo(Succeed())
				Expect(err.Error()).To(MatchRegexp("duplicate task name 'a' detected"))

				Expect(taskStates).To(HaveLen(0))
			})
		})
	})
}
