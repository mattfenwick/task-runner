package pkg

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
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
	c := setKeyCountTask("c", "c", ran, dAgain)
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
	dAgain := SetKeyTask("d-again", "d", ran,
		SetKeyTask("d", "d", ran))
	a := SetKeyTask("a", "a", ran,
		SetKeyTask("b", "b", ran, dAgain),
		SetKeyTask("c", "c", ran, dAgain),
		dAgain)
	return ran, a
}

func setKeyTwicePrereqGraph() (map[string]bool, Task) {
	ran := map[string]bool{}
	dAgain := SetKeyTaskPrereq("d-again", "d", ran,
		SetKeyTaskPrereq("d", "d", ran))
	a := SetKeyTaskPrereq("a", "a", ran,
		SetKeyTaskPrereq("b", "b", ran, dAgain),
		SetKeyTaskPrereq("c", "c", ran, dAgain),
		dAgain)
	return ran, a
}

func RunSimpleTaskRunnerTests() {
	Describe("TaskRunner", func() {
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

			TaskDebugPrint(rootTask)

			statuses, err := (&SimpleTaskRunner{}).TaskRunnerRun(rootTask, false)
			Expect(err).To(Succeed())

			for _, s := range []string{"a", "b", "c", "d"} {
				Expect(dict[s]).To(BeTrue())
				log.Infof("key: %s", s)
				Expect(tasks[s].RunCount).To(Equal(int64(1)))
			}
			Expect(dict).ToNot(HaveKey("d-again"))
			Expect(tasks).To(HaveKey("d-again"))
			Expect(tasks["d-again"].RunCount).To(Equal(int64(0)))

			Expect(statuses).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-a", SimpleTaskRunnerTaskStateComplete))
			Expect(statuses).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-b", SimpleTaskRunnerTaskStateComplete))
			Expect(statuses).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-c", SimpleTaskRunnerTaskStateComplete))
			Expect(statuses).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-d", SimpleTaskRunnerTaskStateComplete))
			Expect(statuses).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-d-again", SimpleTaskRunnerTaskStateSkipped))
		})

		It("fails to execute tasks whose prereqs fail", func() {
			dict, t := setKeyTwicePrereqGraph()

			statuses, err := (&SimpleTaskRunner{}).TaskRunnerRun(t, false)
			Expect(err).NotTo(Succeed())
			Expect(err.Error()).To(MatchRegexp("prereq 'prereq-keycheck-task-setkey-d-again' failed for task task-setkey-d-again: key d already in dict"))

			// only d runs
			Expect(dict).To(HaveKeyWithValue("d", true))
			// everything else fails
			for _, s := range []string{"a", "b", "c", "e"} {
				Expect(dict).NotTo(HaveKey(s))
			}

			Expect(statuses).To(HaveKeyWithValue("task-setkey-a", SimpleTaskRunnerTaskStateWaiting))
			Expect(statuses).To(HaveKeyWithValue("task-setkey-b", SimpleTaskRunnerTaskStateWaiting))
			Expect(statuses).To(HaveKeyWithValue("task-setkey-c", SimpleTaskRunnerTaskStateWaiting))
			Expect(statuses).To(HaveKeyWithValue("task-setkey-d", SimpleTaskRunnerTaskStateComplete))
			Expect(statuses).To(HaveKeyWithValue("task-setkey-d-again", SimpleTaskRunnerTaskStateFailed))
		})

		It("fails to execute tasks whose IsDone fails", func() {
			errorMessage := "test error: pre-IsDone check reports error"
			t := NewFunctionTask("task-isdone-failure", func() error { return nil }, []Task{}, []Prereq{}, func() (b bool, err error) {
				return false, errors.Errorf(errorMessage)
			})

			statuses, err := (&SimpleTaskRunner{}).TaskRunnerRun(t, false)

			Expect(err).NotTo(Succeed())
			Expect(err.Error()).To(MatchRegexp(errorMessage))

			Expect(statuses).To(HaveKeyWithValue("task-isdone-failure", SimpleTaskRunnerTaskStateFailed))
		})

		runIsDoneFailureTask := func(name string, shouldError bool, deps ...Task) Task {
			isDone := false
			return NewFunctionTask(fmt.Sprintf("task-runIsDoneFailureTask-%s", name), func() error {
				isDone = true
				return nil
			}, deps, []Prereq{}, func() (b bool, err error) {
				if !isDone {
					return false, nil
				}
				if shouldError {
					return false, errors.Errorf("test error: predetermined IsDone failure")
				}
				return false, nil
			})
		}

		It("fails after executing a task, if the post-IsDone check returns false", func() {
			t := runIsDoneFailureTask("task-isdone-false", false)

			statuses, err := (&SimpleTaskRunner{}).TaskRunnerRun(t, false)

			Expect(err).NotTo(Succeed())
			Expect(err.Error()).To(MatchRegexp("ran task task-runIsDoneFailureTask-task-isdone-false but it still reports itself as not done"))

			Expect(statuses).To(HaveKeyWithValue("task-runIsDoneFailureTask-task-isdone-false", SimpleTaskRunnerTaskStateFailed))
		})

		It("fails after executing a task, if the post-IsDone check errors", func() {
			t := runIsDoneFailureTask("task-isdone-false", true)

			statuses, err := (&SimpleTaskRunner{}).TaskRunnerRun(t, false)

			Expect(err).NotTo(Succeed())
			Expect(err.Error()).To(MatchRegexp("task task-runIsDoneFailureTask-task-isdone-false failed post-execution IsDone check: test error: predetermined IsDone failure"))

			Expect(statuses).To(HaveKeyWithValue("task-runIsDoneFailureTask-task-isdone-false", SimpleTaskRunnerTaskStateFailed))
		})

		Describe("Task failures", func() {
			It("fails to execute tasks whose deps fail", func() {
				dict, t := setKeyTwiceGraph()

				statuses, err := (&SimpleTaskRunner{}).TaskRunnerRun(t, false)
				Expect(err).NotTo(Succeed())
				Expect(err.Error()).To(MatchRegexp("failed to run task task-setkey-d-again: key d already in dict"))

				// only d runs
				Expect(dict).To(HaveKeyWithValue("d", true))
				// everything else fails
				for _, s := range []string{"a", "b", "c", "e"} {
					Expect(dict).NotTo(HaveKey(s))
				}

				Expect(statuses).To(HaveKeyWithValue("task-setkey-a", SimpleTaskRunnerTaskStateWaiting))
				Expect(statuses).To(HaveKeyWithValue("task-setkey-b", SimpleTaskRunnerTaskStateWaiting))
				Expect(statuses).To(HaveKeyWithValue("task-setkey-c", SimpleTaskRunnerTaskStateWaiting))
				Expect(statuses).To(HaveKeyWithValue("task-setkey-d-again", SimpleTaskRunnerTaskStateFailed))
				Expect(statuses).To(HaveKeyWithValue("task-setkey-d", SimpleTaskRunnerTaskStateComplete))
			})
		})
	})
}
