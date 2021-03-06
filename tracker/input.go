package tracker

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
	Stop() bool
}

//output source db es influxdb

type OutputSource interface {
	InitHelper(*GlobalConfig)
	LoopProcess()
	ReceiveMsg(interface{})
	Clean()
	Stop() bool
}

// type factory func() interface{}

// var Ins = make(map[string]func() interface{})
var Ins = make(map[string]func() InputSource)

func RegisterIns(name string, f func() InputSource) {
	if _, ok := Ins[name]; ok {
		panic(fmt.Errorf("IntputSource %s is registered", name))
	} else {
		fmt.Println("register ins success: ", name, f)
	}

	Ins[name] = f
}

var Ous = make(map[string]func() OutputSource)

func RegisterOus(name string, f func() OutputSource) {
	if _, ok := Ous[name]; ok {
		panic(fmt.Errorf("OutputSource %s is registered", name))
	} else {
		fmt.Println("register ous success: ", name, f)
	}

	Ous[name] = f
}
