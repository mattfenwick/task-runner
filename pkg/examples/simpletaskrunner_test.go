package examples

import (
	"fmt"
	"github.com/mattfenwick/task-runner/pkg/task-runner"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func resultsToStatuses(results map[string]*task_runner.TaskRunResult) map[string]task_runner.TaskState {
	states := map[string]task_runner.TaskState{}
	for name, result := range results {
		states[name] = result.State
	}
	return states
}

func RunSimpleTaskRunnerTests() {
	Describe("TaskRunner", func() {
		It("runs each not-done task exactly once", func() {
			dict, t := SetKeyOnceGraph(0)

			_, err := (&task_runner.SimpleTaskRunner{}).TaskRunnerRun(t, false)
			Expect(err).To(Succeed())

			for _, s := range []string{"a", "b", "c", "d", "e"} {
				Expect(dict[s]).To(BeTrue())
			}
		})

		It("skips tasks that are already done", func() {
			dict, tasks, rootTask := SetKeyCountGraph()

			task_runner.TaskDebugPrint(rootTask)

			results, err := (&task_runner.SimpleTaskRunner{}).TaskRunnerRun(rootTask, false)
			Expect(err).To(Succeed())

			for _, s := range []string{"a", "b", "c", "d"} {
				Expect(dict[s]).To(BeTrue())
				log.Infof("key: %s", s)
				Expect(tasks[s].RunCount).To(Equal(int64(1)))
			}
			Expect(dict).ToNot(HaveKey("d-again"))
			Expect(tasks).To(HaveKey("d-again"))
			Expect(tasks["d-again"].RunCount).To(Equal(int64(0)))

			statuses := resultsToStatuses(results)
			Expect(statuses).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-a", task_runner.TaskStateComplete))
			Expect(statuses).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-b", task_runner.TaskStateComplete))
			Expect(statuses).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-c", task_runner.TaskStateComplete))
			Expect(statuses).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-d", task_runner.TaskStateComplete))
			Expect(statuses).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-d-again", task_runner.TaskStateSkipped))
		})

		It("fails to execute tasks whose prereqs fail", func() {
			dict, t := SetKeyTwicePrereqGraph()

			results, err := (&task_runner.SimpleTaskRunner{}).TaskRunnerRun(t, false)
			Expect(err).NotTo(Succeed())
			Expect(err.Error()).To(MatchRegexp("prereq 'prereq-keycheck-task-setkey-d-again' failed for task task-setkey-d-again: key d already in dict"))

			// only d runs
			Expect(dict).To(HaveKeyWithValue("d", true))
			// everything else fails
			for _, s := range []string{"a", "b", "c", "e"} {
				Expect(dict).NotTo(HaveKey(s))
			}

			statuses := resultsToStatuses(results)
			Expect(statuses).To(HaveKeyWithValue("task-setkey-a", task_runner.TaskStateWaiting))
			Expect(statuses).To(HaveKeyWithValue("task-setkey-b", task_runner.TaskStateWaiting))
			Expect(statuses).To(HaveKeyWithValue("task-setkey-c", task_runner.TaskStateWaiting))
			Expect(statuses).To(HaveKeyWithValue("task-setkey-d", task_runner.TaskStateComplete))
			Expect(statuses).To(HaveKeyWithValue("task-setkey-d-again", task_runner.TaskStateFailed))
		})

		It("fails to execute tasks whose IsDone fails", func() {
			errorMessage := "test error: pre-IsDone check reports error"
			t := task_runner.NewFunctionTask("task-isdone-failure", func() error { return nil }, []task_runner.Task{}, []task_runner.Prereq{}, func() (b bool, err error) {
				return false, errors.Errorf(errorMessage)
			})

			results, err := (&task_runner.SimpleTaskRunner{}).TaskRunnerRun(t, false)

			Expect(err).NotTo(Succeed())
			Expect(err.Error()).To(MatchRegexp(errorMessage))

			statuses := resultsToStatuses(results)
			Expect(statuses).To(HaveKeyWithValue("task-isdone-failure", task_runner.TaskStateFailed))
		})

		runIsDoneFailureTask := func(name string, shouldError bool, deps ...task_runner.Task) task_runner.Task {
			isDone := false
			return task_runner.NewFunctionTask(fmt.Sprintf("task-runIsDoneFailureTask-%s", name), func() error {
				isDone = true
				return nil
			}, deps, []task_runner.Prereq{}, func() (b bool, err error) {
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

			results, err := (&task_runner.SimpleTaskRunner{}).TaskRunnerRun(t, false)

			Expect(err).NotTo(Succeed())
			Expect(err.Error()).To(MatchRegexp("ran task task-runIsDoneFailureTask-task-isdone-false but it still reports itself as not done"))

			statuses := resultsToStatuses(results)
			Expect(statuses).To(HaveKeyWithValue("task-runIsDoneFailureTask-task-isdone-false", task_runner.TaskStateFailed))
		})

		It("fails after executing a task, if the post-IsDone check errors", func() {
			t := runIsDoneFailureTask("task-isdone-false", true)

			results, err := (&task_runner.SimpleTaskRunner{}).TaskRunnerRun(t, false)

			Expect(err).NotTo(Succeed())
			Expect(err.Error()).To(MatchRegexp("task task-runIsDoneFailureTask-task-isdone-false failed post-execution IsDone check: test error: predetermined IsDone failure"))

			statuses := resultsToStatuses(results)
			Expect(statuses).To(HaveKeyWithValue("task-runIsDoneFailureTask-task-isdone-false", task_runner.TaskStateFailed))
		})

		Describe("Task failures", func() {
			It("fails to execute tasks whose deps fail", func() {
				dict, t := SetKeyTwiceGraph()

				results, err := (&task_runner.SimpleTaskRunner{}).TaskRunnerRun(t, false)
				Expect(err).NotTo(Succeed())
				Expect(err.Error()).To(MatchRegexp("failed to run task task-setkey-d-again: key d already in dict"))

				// only d runs
				Expect(dict).To(HaveKeyWithValue("d", true))
				// everything else fails
				for _, s := range []string{"a", "b", "c", "e"} {
					Expect(dict).NotTo(HaveKey(s))
				}

				statuses := resultsToStatuses(results)
				Expect(statuses).To(HaveKeyWithValue("task-setkey-a", task_runner.TaskStateWaiting))
				Expect(statuses).To(HaveKeyWithValue("task-setkey-b", task_runner.TaskStateWaiting))
				Expect(statuses).To(HaveKeyWithValue("task-setkey-c", task_runner.TaskStateWaiting))
				Expect(statuses).To(HaveKeyWithValue("task-setkey-d-again", task_runner.TaskStateFailed))
				Expect(statuses).To(HaveKeyWithValue("task-setkey-d", task_runner.TaskStateComplete))
			})
		})
	})
}
