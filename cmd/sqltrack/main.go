package main

import (
	"flag"
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
	configPath = flag.String("config", "/tmp/sqltrack.toml", "config must be file")
)

func main() {

	flag.Parse()

	globals := tracker.LoadConfig(configPath)

	setRuntime(globals)

	isfactory := tracker.Ins[globals.Base.Input]()

	var (
		is tracker.InputSource
		ok bool
	)

	if is, ok = isfactory.(tracker.InputSource); !ok {
		log.Fatalln("tracker may not initiatial!!!")
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

func process(is tracker.InputSource, t *tracker.Tracker) {
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

func setRuntime(g *tracker.GlobalConfig) {
	if g.Base.MaxCpu > 0 && g.Base.MaxCpu <= 32 {
		runtime.GOMAXPROCS(g.Base.MaxCpu)
	}
}
