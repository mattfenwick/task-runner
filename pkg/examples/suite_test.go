package examples

import (
	log "github.com/sirupsen/logrus"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPkg(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	RegisterFailHandler(Fail)
	RunTaskTests()
	RunDfsTests()
	RunSimpleTaskRunnerTests()
	RunParallelTaskRunnerTests()
	RunTraversalsTests()
	RunSpecs(t, "task-runner")
}
