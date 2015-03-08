package main

import (
	"flag"
	"fmt"
	"github.com/dongzerun/sqltrack/input"
	"github.com/dongzerun/sqltrack/message"
	"github.com/golang/protobuf/proto"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	configPath = flag.String("config", "/tmp/kafka.toml", "config must be file")
)

func main() {

	flag.Parse()
	// globals := loadConfig(configPath)
	log.Println("start decode config file")
	globals := input.LoadConfig(configPath)
	log.Println(globals.KafkaConfig)
	log.Println(globals.KafkaConfig.Addrs)

	// var is input.InputSource
	// is = input.NewKafkaHelper(globals)
	// 	cmd/sqltrack/main.go:30: too many arguments in call to input.Ins[globals.Base.Input]
	// cmd/sqltrack/main.go:30: cannot use input.Ins[globals.Base.Input](globals) (type interface {}) as type input.InputSource in assignment:
	//         interface {} does not implement input.InputSource (missing Clean method)
	factory := input.Ins[globals.Base.Input]()
	log.Println("input is: ", globals.Base.Input)

	var is input.InputSource
	var ok bool

	if is, ok = factory.(input.InputSource); !ok {
		log.Fatalln("input may not initiatial!!!")
	}
	// is := &input.KafkaHelper{}

	// cmd/sqltrack/main.go:41: cannot use globals (type *input.GlobalConfig)
	// as type input.GlobalConfig in argument to is.InitHelper
	is.InitHelper(globals)
	go is.StartPull()

	go func() {
		for {
			select {
			case data := <-is.Consume():
				fmt.Println(data.GetOffset())
				msg := &message.Message{}
				proto.Unmarshal(data.GetValue(), msg)
				fmt.Println(msg)
			case <-is.Stop():
				return
			}
		}
	}()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	<-sc
	is.Clean()
}
