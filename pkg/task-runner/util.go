package task_runner

import (
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"os"
	"os/exec"
)

func ObjectId(o interface{}) string {
	return fmt.Sprintf("%p", o)
}

func RunCommand(cmd *exec.Cmd) (string, error) {
	log.Infof("running command '%s' in directory: '%s'", cmd.String(), cmd.Dir)
	cmdOutput, err := cmd.CombinedOutput()
	cmdOutputStr := string(cmdOutput)
	log.Tracef("command '%s' output:\n%s", cmd.String(), cmdOutput)
	return cmdOutputStr, errors.Wrapf(err, "unable to run command '%s': %s", cmd.String(), cmdOutputStr)
}

func SetCommandDirectoryIfNotSet(cmd *exec.Cmd) error {
	if len(cmd.Dir) != 0 {
		return nil
	}
	dir, err := os.Executable()
	if err != nil {
		return errors.Wrapf(err, "unable to get path to executable")
	}
	cmd.Dir = dir
	return nil
}

func AddEnvironmentToCommand(cmd *exec.Cmd, env map[string]string) {
	if cmd.Env == nil {
		cmd.Env = os.Environ()
	}
	for key, val := range env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", key, val))
	}
}

// RunCommandAndPrint runs a command and connects its stdout and stderr to that of the parent program.
// See:
//   https://stackoverflow.com/questions/8875038/redirect-stdout-pipe-of-child-process-in-go
//   https://stackoverflow.com/questions/25190971/golang-copy-exec-output-to-log/25191479#25191479
func RunCommandAndPrint(cmd *exec.Cmd) error {
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	// can't use RunCommand(cmd) here -- attaching to os pipes interferes with cmd.CombinedOutput()
	log.Infof("running command '%s' with pipes attached in directory: '%s'", cmd.String(), cmd.Dir)
	return errors.Wrapf(cmd.Run(), "unable to run command '%s'", cmd.String())
}
