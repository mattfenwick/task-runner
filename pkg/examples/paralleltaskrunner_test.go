package examples

import (
	"context"
	"fmt"
	tr "github.com/mattfenwick/task-runner/pkg/task-runner"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func RunParallelTaskRunnerTests() {
	Describe("ParallelTaskRunner", func() {
		It("runs each not-done task exactly once", func() {
			dict, t := SetKeyOnceGraph()

			runner, err := tr.NewDefaultParallelTaskRunner(t, 2)
			Expect(err).To(Succeed())
			_ = runner.Wait(context.TODO())

			for _, s := range []string{"a", "b", "c", "d", "e"} {
				Expect(dict[s]).To(BeTrue())
			}
		})

		It("skips tasks that are already done", func() {
			dict, tasks, rootTask := SetKeyCountGraph()

			tr.TaskDebugPrint(rootTask)

			runner, err := tr.NewParallelTaskRunner(rootTask, 1, 1000)
			Expect(err).To(Succeed())
			results := runner.Wait(context.TODO())

			for _, s := range []string{"a", "b", "c", "d"} {
				Expect(dict).To(HaveKeyWithValue(s, true))
				log.Infof("key: %s (%+v)", s, dict)
				Expect(tasks[s].RunCount).To(Equal(int64(1)))
			}
			Expect(dict).ToNot(HaveKey("d-again"))
			Expect(tasks).To(HaveKey("d-again"))
			Expect(tasks["d-again"].RunCount).To(Equal(int64(0)))

			Expect(results["RunCountTask-wrapper-task-setkeyidempotent-a"].State).To(Equal(tr.TaskStateComplete))
			Expect(results["RunCountTask-wrapper-task-setkeyidempotent-b"].State).To(Equal(tr.TaskStateComplete))
			Expect(results["RunCountTask-wrapper-task-setkeyidempotent-c"].State).To(Equal(tr.TaskStateComplete))
			Expect(results["RunCountTask-wrapper-task-setkeyidempotent-d"].State).To(Equal(tr.TaskStateComplete))
			Expect(results["RunCountTask-wrapper-task-setkeyidempotent-d-again"].State).To(Equal(tr.TaskStateSkipped))
		})

		It("fails to execute tasks whose prereqs fail", func() {
			dict, t := SetKeyTwicePrereqGraph()

			runner, err := tr.NewParallelTaskRunner(t, 2, 1000)
			Expect(err).To(Succeed())
			results := runner.Wait(context.TODO())

			//Expect(err.Error()).To(MatchRegexp("prereq 'prereq-keycheck-task-setkey-d' failed for task task-setkey-d: key d already in dict"))

			// only d runs
			Expect(dict).To(HaveKeyWithValue("d", true))
			// everything else fails
			for _, s := range []string{"a", "b", "c", "e"} {
				Expect(dict).NotTo(HaveKey(s))
			}

			Expect(results["task-setkey-a"].State).To(Equal(tr.TaskStateWaiting))
			Expect(results["task-setkey-b"].State).To(Equal(tr.TaskStateWaiting))
			Expect(results["task-setkey-c"].State).To(Equal(tr.TaskStateWaiting))
			Expect(results["task-setkey-d"].State).To(Equal(tr.TaskStateComplete))
			Expect(results["task-setkey-d-again"].State).To(Equal(tr.TaskStateFailed))
		})

		It("fails to execute tasks whose IsDone fails", func() {
			errorMessage := "test error: pre-IsDone check reports error"
			t := tr.NewFunctionTask("task-isdone-failure", func() error { return nil }, []tr.Task{}, []tr.Prereq{}, func() (b bool, err error) {
				return false, errors.Errorf(errorMessage)
			})

			runner, err := tr.NewParallelTaskRunner(t, 2, 100)
			Expect(err).To(Succeed())
			_ = runner.Wait(context.TODO())

			Expect(runner.Tasks["task-isdone-failure"].State).To(Equal(tr.TaskStateFailed))
		})

		runIsDoneFailureTask := func(name string, shouldError bool, deps ...tr.Task) tr.Task {
			isDone := false
			return tr.NewFunctionTask(fmt.Sprintf("task-runIsDoneFailureTask-%s", name), func() error {
				isDone = true
				return nil
			}, deps, []tr.Prereq{}, func() (b bool, err error) {
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

			runner, err := tr.NewParallelTaskRunner(t, 2, 100)
			Expect(err).To(Succeed())
			_ = runner.Wait(context.TODO())

			Expect(runner.Tasks["task-runIsDoneFailureTask-task-isdone-false"].State).To(Equal(tr.TaskStateFailed))
		})

		It("fails after executing a task, if the post-IsDone check errors", func() {
			t := runIsDoneFailureTask("task-isdone-false", true)

			runner, err := tr.NewParallelTaskRunner(t, 2, 100)
			Expect(err).To(Succeed())
			_ = runner.Wait(context.TODO())

			Expect(runner.Tasks["task-runIsDoneFailureTask-task-isdone-false"].State).To(Equal(tr.TaskStateFailed))
		})

		Describe("Task failures", func() {
			It("fails to execute tasks whose deps fail", func() {
				dict, t := SetKeyTwiceGraph()

				runner, err := tr.NewParallelTaskRunner(t, 2, 100)
				Expect(err).To(Succeed())
				_ = runner.Wait(context.TODO())

				// only d runs
				Expect(dict).To(HaveKeyWithValue("d", true))
				// everything else fails
				for _, s := range []string{"a", "b", "c", "e"} {
					Expect(dict).NotTo(HaveKey(s))
				}

				Expect(runner.Tasks["task-setkey-a"].State).To(Equal(tr.TaskStateWaiting))
				Expect(runner.Tasks["task-setkey-b"].State).To(Equal(tr.TaskStateWaiting))
				Expect(runner.Tasks["task-setkey-c"].State).To(Equal(tr.TaskStateWaiting))
				Expect(runner.Tasks["task-setkey-d"].State).To(Equal(tr.TaskStateComplete))
				Expect(runner.Tasks["task-setkey-d-again"].State).To(Equal(tr.TaskStateFailed))
			})
		})
	})
}
