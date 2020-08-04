package pkg

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"sync"
)

func RunParallelTaskRunnerTests() {
	Describe("ParallelTaskRunner", func() {
		It("runs each not-done task exactly once", func() {
			dict, t := setKeyOnceGraph()

			wg := &sync.WaitGroup{}
			wg.Add(5)
			didFinishTask := func(t Task, s ParallelTaskRunnerTaskState, err error) {
				log.Infof("ParallelTaskRunner: finish %s, %s, %+v", t.TaskName(), s.String(), err)
				wg.Done()
			}
			runner := NewParallelTaskRunner(2, didFinishTask)

			Expect(runner.AddTask(t)).To(Succeed())
			Expect(runner.Run()).To(Succeed())

			wg.Wait()
			for _, s := range []string{"a", "b", "c", "d", "e"} {
				Expect(dict[s]).To(BeTrue())
			}
		})

		It("skips tasks that are already done", func() {
			dict, tasks, rootTask := setKeyCountGraph()

			TaskDebugPrint(rootTask)

			wg := &sync.WaitGroup{}
			wg.Add(5)
			states := map[string]ParallelTaskRunnerTaskState{}
			didFinishTask := func(t Task, s ParallelTaskRunnerTaskState, err error) {
				log.Infof("ParallelTaskRunner: finish %s, %s, %+v", t.TaskName(), s.String(), err)
				states[t.TaskName()] = s
				wg.Done()
			}
			runner := NewParallelTaskRunner(1, didFinishTask)

			Expect(runner.AddTask(rootTask)).To(Succeed())
			Expect(runner.Run()).To(Succeed())

			// uncomment for debugging help
			//jbytes, err := json.MarshalIndent(runner, "", "  ")
			//Expect(err).To(Succeed())
			//fmt.Printf("%s\n", jbytes)

			wg.Wait()

			for _, s := range []string{"a", "b", "c", "d"} {
				Expect(dict).To(HaveKeyWithValue(s, true))
				log.Infof("key: %s (%+v)", s, dict)
				Expect(tasks[s].RunCount).To(Equal(int64(1)))
			}
			Expect(dict).ToNot(HaveKey("d-again"))
			Expect(tasks).To(HaveKey("d-again"))
			Expect(tasks["d-again"].RunCount).To(Equal(int64(0)))

			Expect(states).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-a", ParallelTaskRunnerTaskStateComplete))
			Expect(states).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-b", ParallelTaskRunnerTaskStateComplete))
			Expect(states).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-c", ParallelTaskRunnerTaskStateComplete))
			Expect(states).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-d", ParallelTaskRunnerTaskStateComplete))
			Expect(states).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-d-again", ParallelTaskRunnerTaskStateSkipped))
		})

		It("fails to execute tasks whose prereqs fail", func() {
			dict, t := setKeyTwicePrereqGraph()

			wg := &sync.WaitGroup{}
			wg.Add(2)
			states := map[string]ParallelTaskRunnerTaskState{}
			runner := NewParallelTaskRunner(2, func(t Task, s ParallelTaskRunnerTaskState, err error) {
				log.Infof("ParallelTaskRunner: finish %s, %s, %+v", t.TaskName(), s.String(), err)
				states[t.TaskName()] = s
				wg.Done()
			})

			Expect(runner.AddTask(t)).To(Succeed())
			err := runner.Run()

			Expect(err).To(Succeed())
			//Expect(err.Error()).To(MatchRegexp("prereq 'prereq-keycheck-task-setkey-d' failed for task task-setkey-d: key d already in dict"))

			wg.Wait()
			// only d runs
			Expect(dict).To(HaveKeyWithValue("d", true))
			// everything else fails
			for _, s := range []string{"a", "b", "c", "e"} {
				Expect(dict).NotTo(HaveKey(s))
			}

			Expect(runner.Tasks["task-setkey-a"].State).To(Equal(ParallelTaskRunnerTaskStateWaiting))
			Expect(runner.Tasks["task-setkey-b"].State).To(Equal(ParallelTaskRunnerTaskStateWaiting))
			Expect(runner.Tasks["task-setkey-c"].State).To(Equal(ParallelTaskRunnerTaskStateWaiting))
			Expect(runner.Tasks["task-setkey-d"].State).To(Equal(ParallelTaskRunnerTaskStateComplete))
			Expect(runner.Tasks["task-setkey-d-again"].State).To(Equal(ParallelTaskRunnerTaskStateFailed))
		})

		It("fails to execute tasks whose IsDone fails", func() {
			errorMessage := "test error: pre-IsDone check reports error"
			t := NewFunctionTask("task-isdone-failure", func() error { return nil }, []Task{}, []Prereq{}, func() (b bool, err error) {
				return false, errors.Errorf(errorMessage)
			})

			wg := &sync.WaitGroup{}
			wg.Add(1)
			runner := NewParallelTaskRunner(2, func(task Task, state ParallelTaskRunnerTaskState, err error) {
				wg.Done()
			})

			Expect(runner.AddTask(t)).To(Succeed())
			runner.Run()

			wg.Wait()

			Expect(runner.Tasks["task-isdone-failure"].State).To(Equal(ParallelTaskRunnerTaskStateFailed))
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

			wg := &sync.WaitGroup{}
			wg.Add(1)
			runner := NewParallelTaskRunner(2, func(task Task, state ParallelTaskRunnerTaskState, err error) {
				wg.Done()
			})

			Expect(runner.AddTask(t)).To(Succeed())
			Expect(runner.Run()).To(Succeed())
			wg.Wait()

			Expect(runner.Tasks["task-runIsDoneFailureTask-task-isdone-false"].State).To(Equal(ParallelTaskRunnerTaskStateFailed))
		})

		It("fails after executing a task, if the post-IsDone check errors", func() {
			t := runIsDoneFailureTask("task-isdone-false", true)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			runner := NewParallelTaskRunner(2, func(task Task, state ParallelTaskRunnerTaskState, err error) {
				wg.Done()
			})

			Expect(runner.AddTask(t)).To(Succeed())
			Expect(runner.Run()).To(Succeed())
			wg.Wait()

			Expect(runner.Tasks["task-runIsDoneFailureTask-task-isdone-false"].State).To(Equal(ParallelTaskRunnerTaskStateFailed))
		})

		Describe("Task failures", func() {
			It("fails to execute tasks whose deps fail", func() {
				dict, t := setKeyTwiceGraph()

				wg := &sync.WaitGroup{}
				wg.Add(2)
				runner := NewParallelTaskRunner(2, func(task Task, state ParallelTaskRunnerTaskState, err error) {
					wg.Done()
				})

				Expect(runner.AddTask(t)).To(Succeed())
				Expect(runner.Run()).To(Succeed())
				wg.Wait()

				// only d runs
				Expect(dict).To(HaveKeyWithValue("d", true))
				// everything else fails
				for _, s := range []string{"a", "b", "c", "e"} {
					Expect(dict).NotTo(HaveKey(s))
				}

				Expect(runner.Tasks["task-setkey-a"].State).To(Equal(ParallelTaskRunnerTaskStateWaiting))
				Expect(runner.Tasks["task-setkey-b"].State).To(Equal(ParallelTaskRunnerTaskStateWaiting))
				Expect(runner.Tasks["task-setkey-c"].State).To(Equal(ParallelTaskRunnerTaskStateWaiting))
				Expect(runner.Tasks["task-setkey-d"].State).To(Equal(ParallelTaskRunnerTaskStateComplete))
				Expect(runner.Tasks["task-setkey-d-again"].State).To(Equal(ParallelTaskRunnerTaskStateFailed))
			})
		})
	})
}
