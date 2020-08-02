package pkg

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

func setKeyOnceGraph() (map[string]bool, Task) {
	ran := map[string]bool{}
	d := SetKeyTask("d", "d", ran,
		SetKeyTask("e", "e", ran))
	a := SetKeyTask("a", "a", ran,
		SetKeyTask("b", "b", ran, d),
		SetKeyTask("c", "c", ran),
		d)
	return ran, a
}

func setKeyCountTask(name string, key string, dict map[string]bool, deps ...Task) *RunCountTask {
	return NewRunCountTask(SetKeyTaskIdempotent(name, key, dict, deps...))
}

func setKeyCountGraph() (map[string]bool, map[string]*RunCountTask, Task) {
	ran := map[string]bool{}
	d := setKeyCountTask("d", "d", ran)
	dAgain := setKeyCountTask("d-again", "d", ran, d)
	b := setKeyCountTask("b", "b", ran, d)
	c := setKeyCountTask("c", "c", ran)
	a := setKeyCountTask("a", "a", ran,
		b,
		c,
		d)
	tasks := map[string]*RunCountTask{
		"a":       a,
		"b":       b,
		"c":       c,
		"d":       d,
		"d-again": dAgain,
	}
	return ran, tasks, a
}

func setKeyTwiceGraph() (map[string]bool, Task) {
	ran := map[string]bool{}
	d := SetKeyTask("d", "d", ran,
		SetKeyTask("d-again", "d", ran))
	a := SetKeyTask("a", "a", ran,
		SetKeyTask("b", "b", ran, d),
		SetKeyTask("c", "c", ran),
		d)
	return ran, a
}

func RunTaskRunnerTests() {
	Describe("TaskRunner", func() {
		// TODO run same tests for ParallelTaskRunner
		It("runs each not-done task exactly once", func() {
			dict, t := setKeyOnceGraph()

			_, err := (&SimpleTaskRunner{}).TaskRunnerRun(t, false)
			Expect(err).To(Succeed())

			for _, s := range []string{"a", "b", "c", "d", "e"} {
				Expect(dict[s]).To(BeTrue())
			}
		})

		It("skips tasks that are already done", func() {
			dict, tasks, rootTask := setKeyCountGraph()

			_, err := (&SimpleTaskRunner{}).TaskRunnerRun(rootTask, false)
			Expect(err).To(Succeed())

			for _, s := range []string{"a", "b", "c", "d"} {
				Expect(dict[s]).To(BeTrue())
				log.Infof("key: %s", s)
				Expect(tasks[s].RunCount).To(Equal(int64(1)))
			}
			Expect(dict).ToNot(HaveKey("d-again"))
			Expect(tasks).To(HaveKey("d-again"))
			Expect(tasks["d-again"].RunCount).To(Equal(int64(0)))
		})

		It("fails to execute tasks whose prereqs fail", func() {

		})

		Describe("Task failures", func() {
			It("fails to execute tasks whose deps fail", func() {
				dict, t := setKeyTwiceGraph()

				_, err := (&SimpleTaskRunner{}).TaskRunnerRun(t, false)
				Expect(err).NotTo(Succeed())
				Expect(err.Error()).To(MatchRegexp("failed to run task task-setkey-d: key d already in dict"))

				// only d runs
				Expect(dict).To(HaveKeyWithValue("d", true))
				// everything else fails
				for _, s := range []string{"a", "b", "c", "e"} {
					Expect(dict).NotTo(HaveKey(s))
				}
			})
		})
	})
}
