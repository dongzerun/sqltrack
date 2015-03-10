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

	log.Println("start decode config file")
	globals := input.LoadConfig(configPath)
	log.Println(globals.KafkaConfig)
	log.Println(globals.KafkaConfig.Addrs)

	factory := input.Ins[globals.Base.Input]()
	log.Println("input is: ", globals.Base.Input)

	var is input.InputSource
	var ok bool

	if is, ok = factory.(input.InputSource); !ok {
		log.Fatalln("input may not initiatial!!!")
	}

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
