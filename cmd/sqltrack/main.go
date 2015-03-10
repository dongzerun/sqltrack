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
	"runtime"
	"syscall"
)

var (
	configPath = flag.String("config", "/tmp/kafka.toml", "config must be file")
)

func main() {

	flag.Parse()

	globals := input.LoadConfig(configPath)

	setRuntime(globals)

	isfactory := input.Ins[globals.Base.Input]()

	var is input.InputSource
	var ok bool

	if is, ok = isfactory.(input.InputSource); !ok {
		log.Fatalln("input may not initiatial!!!")
	}

	is.InitHelper(globals)
	go is.StartPull()

	opfactory := input.Ous[globals.Base.Output]()
	var op input.OutputSource

	if op, ok = opfactory.(input.OutputSource); !ok {
		log.Fatalln("output may not initiatial!!!")
	}

	log.Println(op)

	go process(is)

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

func process(is input.InputSource) {
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
}

func setRuntime(g *input.GlobalConfig) {
	if g.Base.MaxCpu > 0 {
		runtime.GOMAXPROCS(g.Base.MaxCpu)
	}
}
