package pkg

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestPkg(t *testing.T) {
	RegisterFailHandler(Fail)
	RunTaskRunnerTests()
	RunSpecs(t, "task-runner")
}
