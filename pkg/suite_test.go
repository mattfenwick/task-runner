package pkg

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestPkg(t *testing.T) {
	RegisterFailHandler(Fail)
	RunTaskRunnerTests()
	RunTaskTests()
	RunSpecs(t, "task-runner")
}
