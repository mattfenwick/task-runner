package pkg

import "github.com/pkg/errors"

type TaskState string

// TODO could also add:
//   canceled by caller or something
//   parse out different kinds of failures (run, isdone, prereq, dep)
const (
	TaskStateReady      TaskState = "TaskStateReady"
	TaskStateWaiting    TaskState = "TaskStateWaiting"
	TaskStateInProgress TaskState = "TaskStateInProgress"
	TaskStateFailed     TaskState = "TaskStateFailed"
	TaskStateSkipped    TaskState = "TaskStateSkipped"
	TaskStateComplete   TaskState = "TaskStateComplete"
)

func (s TaskState) String() string {
	switch s {
	case TaskStateReady:
		return "Ready"
	case TaskStateWaiting:
		return "Waiting"
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
