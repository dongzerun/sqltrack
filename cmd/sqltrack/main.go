package main

import (
	"flag"
	// "fmt"
	"github.com/dongzerun/sqltrack/input"
	"github.com/dongzerun/sqltrack/message"
	"github.com/dongzerun/sqltrack/tracker"
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

	t := tracker.NewTracker()
	t.Init(globals)
	go process(is, t)

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

func process(is input.InputSource, t *tracker.Tracker) {
	for {
		select {
		case data := <-is.Consume():
			// fmt.Println(data.GetOffset())
			msg := &message.Message{}
			proto.Unmarshal(data.GetValue(), msg)
			t.Receive(msg)
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
