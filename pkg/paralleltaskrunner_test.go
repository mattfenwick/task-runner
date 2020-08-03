package pkg

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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

			err := runner.AddTask(t)
			Expect(err).To(Succeed())
			err = runner.Run()
			Expect(err).To(Succeed())

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

			err := runner.AddTask(rootTask)
			Expect(err).To(Succeed())
			err = runner.Run()
			Expect(err).To(Succeed())

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
		//
		//It("fails to execute tasks whose prereqs fail", func() {
		//	dict, t := setKeyTwicePrereqGraph()
		//
		//	statuses, err := (&SimpleTaskRunner{}).TaskRunnerRun(t, false)
		//	Expect(err).NotTo(Succeed())
		//	Expect(err.Error()).To(MatchRegexp("prereq 'prereq-keycheck-task-setkey-d' failed for task task-setkey-d: key d already in dict"))
		//
		//	// only d runs
		//	Expect(dict).To(HaveKeyWithValue("d", true))
		//	// everything else fails
		//	for _, s := range []string{"a", "b", "c", "e"} {
		//		Expect(dict).NotTo(HaveKey(s))
		//	}
		//
		//	Expect(statuses).To(HaveKeyWithValue("task-setkey-a", SimpleTaskRunnerTaskStateWaiting))
		//	Expect(statuses).To(HaveKeyWithValue("task-setkey-b", SimpleTaskRunnerTaskStateWaiting))
		//	Expect(statuses).To(HaveKeyWithValue("task-setkey-c", SimpleTaskRunnerTaskStateWaiting))
		//	Expect(statuses).To(HaveKeyWithValue("task-setkey-d", SimpleTaskRunnerTaskStateFailed))
		//	Expect(statuses).To(HaveKeyWithValue("task-setkey-d-again", SimpleTaskRunnerTaskStateComplete))
		//})
		//
		//var runIsDoneFailureTask = func(name string, shouldError bool, deps ...Task) Task {
		//	isDone := false
		//	return NewFunctionTask(fmt.Sprintf("task-runIsDoneFailureTask-%s", name), func() error {
		//		isDone = true
		//		return nil
		//	}, deps, []Prereq{}, func() (b bool, err error) {
		//		if !isDone {
		//			return false, nil
		//		}
		//		if shouldError {
		//			return false, errors.Errorf("test error: predetermined IsDone failure")
		//		}
		//		return false, nil
		//	})
		//}
		//
		//It("fails to execute tasks whose IsDone fails", func() {
		//	errorMessage := "test error: pre-IsDone check reports error"
		//	t := NewFunctionTask("task-isdone-failure", func() error { return nil }, []Task{}, []Prereq{}, func() (b bool, err error) {
		//		return false, errors.Errorf(errorMessage)
		//	})
		//
		//	statuses, err := (&SimpleTaskRunner{}).TaskRunnerRun(t, false)
		//
		//	Expect(err).NotTo(Succeed())
		//	Expect(err.Error()).To(MatchRegexp(errorMessage))
		//
		//	Expect(statuses).To(HaveKeyWithValue("task-isdone-failure", SimpleTaskRunnerTaskStateFailed))
		//})
		//
		//It("fails after executing a task, if the post-IsDone check returns false", func() {
		//	t := runIsDoneFailureTask("task-isdone-false", false)
		//
		//	statuses, err := (&SimpleTaskRunner{}).TaskRunnerRun(t, false)
		//
		//	Expect(err).NotTo(Succeed())
		//	Expect(err.Error()).To(MatchRegexp("ran task task-runIsDoneFailureTask-task-isdone-false but it still reports itself as not done"))
		//
		//	Expect(statuses).To(HaveKeyWithValue("task-runIsDoneFailureTask-task-isdone-false", SimpleTaskRunnerTaskStateFailed))
		//})
		//
		//It("fails after executing a task, if the post-IsDone check errors", func() {
		//	t := runIsDoneFailureTask("task-isdone-false", true)
		//
		//	statuses, err := (&SimpleTaskRunner{}).TaskRunnerRun(t, false)
		//
		//	Expect(err).NotTo(Succeed())
		//	Expect(err.Error()).To(MatchRegexp("task task-runIsDoneFailureTask-task-isdone-false failed post-execution IsDone check: test error: predetermined IsDone failure"))
		//
		//	Expect(statuses).To(HaveKeyWithValue("task-runIsDoneFailureTask-task-isdone-false", SimpleTaskRunnerTaskStateFailed))
		//})
		//
		//Describe("Task failures", func() {
		//	It("fails to execute tasks whose deps fail", func() {
		//		dict, t := setKeyTwiceGraph()
		//
		//		statuses, err := (&SimpleTaskRunner{}).TaskRunnerRun(t, false)
		//		Expect(err).NotTo(Succeed())
		//		Expect(err.Error()).To(MatchRegexp("failed to run task task-setkey-d: key d already in dict"))
		//
		//		// only d runs
		//		Expect(dict).To(HaveKeyWithValue("d", true))
		//		// everything else fails
		//		for _, s := range []string{"a", "b", "c", "e"} {
		//			Expect(dict).NotTo(HaveKey(s))
		//		}
		//
		//		Expect(statuses).To(HaveKeyWithValue("task-setkey-a", SimpleTaskRunnerTaskStateWaiting))
		//		Expect(statuses).To(HaveKeyWithValue("task-setkey-b", SimpleTaskRunnerTaskStateWaiting))
		//		Expect(statuses).To(HaveKeyWithValue("task-setkey-c", SimpleTaskRunnerTaskStateWaiting))
		//		Expect(statuses).To(HaveKeyWithValue("task-setkey-d", SimpleTaskRunnerTaskStateFailed))
		//		Expect(statuses).To(HaveKeyWithValue("task-setkey-d-again", SimpleTaskRunnerTaskStateComplete))
		//	})
		//})
	})
}
