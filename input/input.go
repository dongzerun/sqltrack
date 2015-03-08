package input

import (
	"fmt"
)

//input msg must contain following methods
type InputMsg interface {
	GetTopic() string
	GetOffset() int64
	GetValue() []byte
	GetKey() []byte
	GetPartition() int32
}

//input source
type InputSource interface {
	InitHelper(*GlobalConfig)
	StartPull()
	Consume() <-chan InputMsg
	Clean()
	Stop() <-chan bool
}

// type factory func() interface{}

// var Ins = make(map[string]func() interface{})
var Ins = make(map[string]func() InputSource)

func Register(name string, f func() InputSource) {
	if _, ok := Ins[name]; ok {
		panic(fmt.Errorf("IntputSource %s is registered", name))
	}

	Ins[name] = f
}
