package pkg

import (
	log "github.com/sirupsen/logrus"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestPkg(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	RegisterFailHandler(Fail)
	RunTaskTests()
	RunSimpleTaskRunnerTests()
	RunParallelTaskRunnerTests()
	RunSpecs(t, "task-runner")
}
