package pkg

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestPkg(t *testing.T) {
	RegisterFailHandler(Fail)
	RunTaskTests()
	RunSimpleTaskRunnerTests()
	RunParallelTaskRunnerTests()
	RunSpecs(t, "task-runner")
}
