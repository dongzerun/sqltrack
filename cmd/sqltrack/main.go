package main

import (
	"flag"
	"github.com/dongzerun/sqltrack/tracker"

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

	t := tracker.NewTracker()
	t.Init(globals)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-sc
	t.Clean()
}

// to be continue ,add cpu and memory profiling
func setRuntime(g *tracker.GlobalConfig) {
	if g.Base.MaxCpu > 0 && g.Base.MaxCpu <= 32 {
		runtime.GOMAXPROCS(g.Base.MaxCpu)
	}
}
