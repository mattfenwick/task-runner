package examples

import (
	"context"
	"encoding/json"
	"fmt"
	tr "github.com/mattfenwick/task-runner/pkg/task-runner"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"time"
)

func RunParallelTaskRunnerTests() {
	Describe("ParallelTaskRunner", func() {
		It("runs each not-done task exactly once", func() {
			dict, t := SetKeyOnceGraph(0)

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
				results := runner.Wait(context.TODO())

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
		})

		Describe("Durations", func() {
			It("Records start, finish and durations properly", func() {
				_, t := SetKeyOnceGraph(1)
				runner, err := tr.NewParallelTaskRunner(t, 3, 100)
				Expect(err).To(Succeed())
				results := runner.Wait(context.TODO())

				for _, task := range results {
					Expect(task.Duration().Seconds()).Should(BeNumerically("~", 1, 0.1))
				}
			})
		})

		Describe("Stop runner while executing", func() {
			It("Should finish off currently executing tasks, but not start any new ones", func() {
				dict, t := SetKeyOnceGraph(5)
				runner, err := tr.NewParallelTaskRunner(t, 3, 100)
				Expect(err).To(Succeed())
				go func() {
					defer GinkgoRecover()
					time.Sleep(2500 * time.Millisecond)
					Expect(runner.Stop()).To(Succeed())
				}()
				results := runner.Wait(context.TODO())

				time.Sleep(7500 * time.Millisecond)

				bytes, err := json.MarshalIndent(results, "", "  ")
				Expect(err).To(Succeed())
				fmt.Printf("%s\n\n", string(bytes))

				// only e runs
				Expect(dict).To(HaveKeyWithValue("e", true))
				// a,b,c,d are not done before cancellation
				for _, s := range []string{"a", "b", "c", "d"} {
					Expect(dict).NotTo(HaveKey(s))
					Expect(results[fmt.Sprintf("task-setkey-%s", s)].State).To(Equal(tr.TaskStateWaiting))
				}

				Expect(results["task-setkey-e"].State).Should(Equal(tr.TaskStateComplete))
				Expect(results["task-setkey-e"].Duration().Seconds()).Should(BeNumerically("~", 5, 0.1))
			})
		})

		Describe("Error recording and handling", func() {
			It("Errors hit at any stage of Task processing are attached to RunnerTask", func() {
				a := tr.NewFunctionTask("a", func() error {
					time.Sleep(2500 * time.Millisecond)
					return errors.Errorf("a failed")
				}, []tr.Task{}, []tr.Prereq{}, func() (bool, error) {
					return false, nil
				})
				b := tr.NewFunctionTask("b", func() error {
					return nil
				}, []tr.Task{}, []tr.Prereq{}, func() (bool, error) {
					time.Sleep(2500 * time.Millisecond)
					return false, errors.Errorf("b isDone before failed")
				})
				cIsDone := false
				c := tr.NewFunctionTask("c", func() error {
					cIsDone = true
					return nil
				}, []tr.Task{}, []tr.Prereq{}, func() (bool, error) {
					if !cIsDone {
						return false, nil
					}
					time.Sleep(2500 * time.Millisecond)
					return false, errors.Errorf("c isDone after failed")
				})
				dIsDone := false
				d := tr.NewFunctionTask("d", func() error {
					dIsDone = true
					return nil
				}, []tr.Task{}, []tr.Prereq{&tr.FunctionPrereq{
					Name: "d-prereq",
					Run: func() error {
						time.Sleep(2500 * time.Millisecond)
						return errors.Errorf("d prereq failed")
					},
				}}, func() (bool, error) {
					return dIsDone, nil
				})
				group := tr.NewNoopTask("group", []tr.Task{a, b, c, d})

				runner, err := tr.NewParallelTaskRunner(group, 5, 100)
				Expect(err).To(Succeed())

				time.Sleep(5 * time.Second)
				runner.Stop() // ignore error
				results := runner.Wait(context.TODO())

				bytes, err := json.MarshalIndent(results, "", "  ")
				Expect(err).To(Succeed())
				fmt.Printf("%s\n\n", string(bytes))

				Expect(results).To(HaveKey("group"))
				// a,b,c,d all failed
				for _, s := range []string{"a", "b", "c", "d"} {
					Expect(results).To(HaveKey(s))
					Expect(results[s].State).To(Equal(tr.TaskStateFailed))
					fmt.Printf("%s: %s\n", s, results[s].Error.Error())
				}
			})
		})
	})
}
