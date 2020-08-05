package pkg

import (
	"encoding/json"
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func RunDfsTests() {
	manyDepsOnATaskDAG := func() Task {
		dAgain := PrintTask("d-again",
			PrintTask("d"))
		a := PrintTask("a",
			dAgain,
			PrintTask("b", dAgain),
			PrintTask("c", dAgain))
		return a
	}

	trivialCycle := func() Task {
		a := PrintTask("a")
		a.TaskAddDependency(a)
		return a
	}

	nonTrivialCycle := func() Task {
		d := PrintTask("d")
		c := PrintTask("c", d)
		b := PrintTask("b", c)
		a := PrintTask("a", b)
		d.TaskAddDependency(a)
		return a
	}

	Describe("BuildDependencyTables", func() {
		Describe("Iterative", func() {
			It("Handles a DAG where a node appears multiple times", func() {
				_, task := setKeyTwiceGraph()
				taskStates, err := BuildDependencyTablesIterative(task)
				Expect(err).To(Succeed())

				Expect(taskStates).To(HaveLen(5))
			})

			It("Handles a DAG where a node is in the stack multiple times at once", func() {
				task := manyDepsOnATaskDAG()
				taskStates, err := BuildDependencyTablesIterative(task)
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
				task := trivialCycle()
				taskStates, err := BuildDependencyTablesIterative(task)
				Expect(err).NotTo(Succeed())
				Expect(err.Error()).To(MatchRegexp("cycle detected"))

				Expect(taskStates).To(HaveLen(0))
			})

			It("Recognizes a non-trivial cycle", func() {
				task := nonTrivialCycle()
				taskStates, err := BuildDependencyTablesIterative(task)
				Expect(err).NotTo(Succeed())
				Expect(err.Error()).To(MatchRegexp("cycle detected"))

				Expect(taskStates).To(HaveLen(0))
			})

			dupeNamesGraph := func() Task {
				a2 := PrintTask("a")
				a := PrintTask("a", a2)
				return a
			}

			It("Recognizes duplicate names", func() {
				task := dupeNamesGraph()
				taskStates, err := BuildDependencyTablesIterative(task)
				Expect(err).NotTo(Succeed())
				Expect(err.Error()).To(MatchRegexp("duplicate task name 'a' detected"))

				Expect(taskStates).To(HaveLen(0))
			})
		})
	})
}
