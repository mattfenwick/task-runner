package task_runner

import "github.com/pkg/errors"

type TaskState string

// TODO could also add:
//   canceled by caller or something
//   parse out different kinds of failures (run, isdone, prereq, dep)
const (
	TaskStateWaiting    TaskState = "TaskStateWaiting"
	TaskStateReady      TaskState = "TaskStateReady"
	TaskStateInProgress TaskState = "TaskStateInProgress"
	TaskStateFailed     TaskState = "TaskStateFailed"
	TaskStateSkipped    TaskState = "TaskStateSkipped"
	TaskStateComplete   TaskState = "TaskStateComplete"
)

func (s TaskState) String() string {
	switch s {
	case TaskStateWaiting:
		return "Waiting"
	case TaskStateReady:
		return "Ready"
	case TaskStateInProgress:
		return "InProgress"
	case TaskStateFailed:
		return "Failed"
	case TaskStateSkipped:
		return "Skipped"
	case TaskStateComplete:
		return "Complete"
	default:
		panic(errors.Errorf("invalid TaskState value %s", string(s)))
	}
}
