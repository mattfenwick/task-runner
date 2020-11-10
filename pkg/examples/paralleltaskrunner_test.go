package examples

import (
	"fmt"
	"github.com/mattfenwick/task-runner/pkg/task-runner"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

// waitTimeout leaks goroutines, don't use it in prod code!
// https://stackoverflow.com/questions/32840687/timeout-for-waitgroup-wait
// It's not super great in tests either :|
func waitTimeout(wg *sync.WaitGroup, timeoutSeconds int) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false
	case <-time.After(time.Duration(timeoutSeconds) * time.Second):
		return true
	}
}

func didUpdateTaskState(wg *sync.WaitGroup) func(t task_runner.Task, s task_runner.TaskState) {
	return func(t task_runner.Task, s task_runner.TaskState) {
		log.Infof("ParallelTaskRunner: finish %s, %s", t.TaskName(), s.String())
		wg.Done()
	}
}

func RunParallelTaskRunnerTests() {
	Describe("ParallelTaskRunner", func() {
		It("runs each not-done task exactly once", func() {
			dict, t := SetKeyOnceGraph()

			wg := &sync.WaitGroup{}
			wg.Add(5)
			runner := task_runner.NewParallelTaskRunner(2, didUpdateTaskState(wg))

			Expect(runner.AddTask(t)).To(Succeed())
			Expect(runner.Start()).To(Succeed())

			waitTimeout(wg, 10)
			for _, s := range []string{"a", "b", "c", "d", "e"} {
				Expect(dict[s]).To(BeTrue())
			}
		})

		It("skips tasks that are already done", func() {
			dict, tasks, rootTask := SetKeyCountGraph()

			task_runner.TaskDebugPrint(rootTask)

			wg := &sync.WaitGroup{}
			wg.Add(5)
			states := map[string]task_runner.TaskState{}
			didFinishTask := func(t task_runner.Task, s task_runner.TaskState) {
				log.Infof("ParallelTaskRunner: finish %s, %s", t.TaskName(), s.String())
				states[t.TaskName()] = s
				wg.Done()
			}
			runner := task_runner.NewParallelTaskRunner(1, didFinishTask)

			Expect(runner.AddTask(rootTask)).To(Succeed())
			Expect(runner.Start()).To(Succeed())

			// uncomment for debugging help
			//jbytes, err := json.MarshalIndent(runner, "", "  ")
			//Expect(err).To(Succeed())
			//fmt.Printf("%s\n", jbytes)

			waitTimeout(wg, 10)

			for _, s := range []string{"a", "b", "c", "d"} {
				Expect(dict).To(HaveKeyWithValue(s, true))
				log.Infof("key: %s (%+v)", s, dict)
				Expect(tasks[s].RunCount).To(Equal(int64(1)))
			}
			Expect(dict).ToNot(HaveKey("d-again"))
			Expect(tasks).To(HaveKey("d-again"))
			Expect(tasks["d-again"].RunCount).To(Equal(int64(0)))

			Expect(states).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-a", task_runner.TaskStateComplete))
			Expect(states).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-b", task_runner.TaskStateComplete))
			Expect(states).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-c", task_runner.TaskStateComplete))
			Expect(states).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-d", task_runner.TaskStateComplete))
			Expect(states).To(HaveKeyWithValue("RunCountTask-wrapper-task-setkeyidempotent-d-again", task_runner.TaskStateSkipped))
		})

		It("fails to execute tasks whose prereqs fail", func() {
			dict, t := SetKeyTwicePrereqGraph()

			wg := &sync.WaitGroup{}
			wg.Add(2)
			states := map[string]task_runner.TaskState{}
			runner := task_runner.NewParallelTaskRunner(2, func(t task_runner.Task, s task_runner.TaskState) {
				log.Infof("ParallelTaskRunner: finish %s, %s", t.TaskName(), s.String())
				states[t.TaskName()] = s
				wg.Done()
			})

			Expect(runner.AddTask(t)).To(Succeed())
			err := runner.Start()

			Expect(err).To(Succeed())
			//Expect(err.Error()).To(MatchRegexp("prereq 'prereq-keycheck-task-setkey-d' failed for task task-setkey-d: key d already in dict"))

			waitTimeout(wg, 10)
			// only d runs
			Expect(dict).To(HaveKeyWithValue("d", true))
			// everything else fails
			for _, s := range []string{"a", "b", "c", "e"} {
				Expect(dict).NotTo(HaveKey(s))
			}

			Expect(runner.Tasks["task-setkey-a"].State).To(Equal(task_runner.TaskStateWaiting))
			Expect(runner.Tasks["task-setkey-b"].State).To(Equal(task_runner.TaskStateWaiting))
			Expect(runner.Tasks["task-setkey-c"].State).To(Equal(task_runner.TaskStateWaiting))
			Expect(runner.Tasks["task-setkey-d"].State).To(Equal(task_runner.TaskStateComplete))
			Expect(runner.Tasks["task-setkey-d-again"].State).To(Equal(task_runner.TaskStateFailed))
		})

		It("fails to execute tasks whose IsDone fails", func() {
			errorMessage := "test error: pre-IsDone check reports error"
			t := task_runner.NewFunctionTask("task-isdone-failure", func() error { return nil }, []task_runner.Task{}, []task_runner.Prereq{}, func() (b bool, err error) {
				return false, errors.Errorf(errorMessage)
			})

			wg := &sync.WaitGroup{}
			wg.Add(1)
			runner := task_runner.NewParallelTaskRunner(2, didUpdateTaskState(wg))

			Expect(runner.AddTask(t)).To(Succeed())
			Expect(runner.Start()).To(Succeed())

			waitTimeout(wg, 10)

			Expect(runner.Tasks["task-isdone-failure"].State).To(Equal(task_runner.TaskStateFailed))
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

			wg := &sync.WaitGroup{}
			wg.Add(1)
			runner := task_runner.NewParallelTaskRunner(2, didUpdateTaskState(wg))

			Expect(runner.AddTask(t)).To(Succeed())
			Expect(runner.Start()).To(Succeed())
			waitTimeout(wg, 10)

			Expect(runner.Tasks["task-runIsDoneFailureTask-task-isdone-false"].State).To(Equal(task_runner.TaskStateFailed))
		})

		It("fails after executing a task, if the post-IsDone check errors", func() {
			t := runIsDoneFailureTask("task-isdone-false", true)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			runner := task_runner.NewParallelTaskRunner(2, didUpdateTaskState(wg))

			Expect(runner.AddTask(t)).To(Succeed())
			Expect(runner.Start()).To(Succeed())
			waitTimeout(wg, 10)

			Expect(runner.Tasks["task-runIsDoneFailureTask-task-isdone-false"].State).To(Equal(task_runner.TaskStateFailed))
		})

		Describe("Task failures", func() {
			It("fails to execute tasks whose deps fail", func() {
				dict, t := SetKeyTwiceGraph()

				wg := &sync.WaitGroup{}
				wg.Add(2)
				runner := task_runner.NewParallelTaskRunner(2, didUpdateTaskState(wg))

				Expect(runner.AddTask(t)).To(Succeed())
				Expect(runner.Start()).To(Succeed())
				waitTimeout(wg, 10)

				// only d runs
				Expect(dict).To(HaveKeyWithValue("d", true))
				// everything else fails
				for _, s := range []string{"a", "b", "c", "e"} {
					Expect(dict).NotTo(HaveKey(s))
				}

				Expect(runner.Tasks["task-setkey-a"].State).To(Equal(task_runner.TaskStateWaiting))
				Expect(runner.Tasks["task-setkey-b"].State).To(Equal(task_runner.TaskStateWaiting))
				Expect(runner.Tasks["task-setkey-c"].State).To(Equal(task_runner.TaskStateWaiting))
				Expect(runner.Tasks["task-setkey-d"].State).To(Equal(task_runner.TaskStateComplete))
				Expect(runner.Tasks["task-setkey-d-again"].State).To(Equal(task_runner.TaskStateFailed))
			})
		})
	})
}
