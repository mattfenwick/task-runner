package pkg

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func validDAG() Task {
	e := PrintTask("e")
	return PrintTask("a",
		PrintTask("b",
			PrintTask("c",
				e),
			PrintTask("d"),
			e))
}

func graphWithTrivialCycle() Task {
	a := PrintTask("a")
	a.TaskAddDependency(a)
	return a
}

func graphWithCycle() Task {
	e := PrintTask("e")
	b := PrintTask("b", PrintTask("d"), e)
	c := PrintTask("c", b, e)
	b.TaskAddDependency(c)
	return PrintTask("a", b)
}

func graphWithDupe() Task {
	return PrintTask("a",
		PrintTask("b",
			PrintTask("c",
				PrintTask("e")),
			PrintTask("d"),
			PrintTask("e")))
}

func taskNames(tasks []Task) []string {
	names := make([]string, len(tasks))
	for i, t := range tasks {
		names[i] = t.TaskName()
	}
	return names
}

func RunTaskTests() {
	Describe("Task", func() {
		Describe("linearize", func() {
			It("linearize", func() {
				tasks, prereqs, err := TaskLinearize(validDAG())
				Expect(err).To(Succeed())
				Expect(taskNames(tasks)).To(Equal([]string{"e", "c", "d", "b", "a"}))
				Expect(prereqs).To(BeNil())
			})

			It("rejects trivial cycles", func() {
				tasks, prereqs, err := TaskLinearize(graphWithTrivialCycle())
				Expect(err).NotTo(Succeed())
				Expect(err.Error()).To(MatchRegexp("cycle"))
				Expect(tasks).To(BeNil())
				Expect(prereqs).To(BeNil())
			})

			It("rejects non-trivial cycles", func() {
				tasks, prereqs, err := TaskLinearize(graphWithCycle())
				Expect(err).NotTo(Succeed())
				Expect(err.Error()).To(MatchRegexp("cycle"))
				Expect(tasks).To(BeNil())
				Expect(prereqs).To(BeNil())
			})

			It("rejects duplicate task names", func() {
				tasks, prereqs, err := TaskLinearize(graphWithDupe())
				Expect(err).NotTo(Succeed())
				Expect(err.Error()).To(MatchRegexp("duplicate"))
				Expect(tasks).To(BeNil())
				Expect(prereqs).To(BeNil())
			})
		})
	})
}
