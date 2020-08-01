package pkg

type Prereq interface {
	PrereqName() string
	PrereqRun() error
}

type FunctionPrereq struct {
	Name string
	Run  func() error
}

func (fp *FunctionPrereq) PrereqName() string {
	return fp.Name
}

func (fp *FunctionPrereq) PrereqRun() error {
	return fp.Run()
}
