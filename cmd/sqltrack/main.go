package main

import (
	"flag"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/dongzerun/sqltrack/tracker"
)

var (
	configPath = flag.String("config", "/tmp/sqltrack.toml", "config must be file")
)

func main() {

	flag.Parse()
	globals := tracker.LoadConfig(configPath)
	setRuntime(globals)

	//start a tracker and init
	t := tracker.NewTracker()
	t.Init(globals)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-sc
	// wait singal and exit
	t.Clean()
}

// to be continue ,add cpu and memory profiling
func setRuntime(g *tracker.GlobalConfig) {
	if g.Base.MaxCpu > 0 && g.Base.MaxCpu <= 32 {
		runtime.GOMAXPROCS(g.Base.MaxCpu)
	}
}
