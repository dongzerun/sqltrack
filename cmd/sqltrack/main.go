package main

import (
	"flag"
	"fmt"
	"github.com/dongzerun/sqltrack/kafka"
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
	globals := loadConfig(configPath)
	log.Println(globals.KafkaConfig)
	log.Println(globals.KafkaConfig.Addrs)

	kh := kafka.NewKafkaHelper(globals.KafkaConfig)
	go kh.StartPull()

	kh.Wg.Wrap(func() {
		for {
			select {
			case data := <-kh.MsgKafka:
				fmt.Println(data.Topic, data.Key, data.Offset, data.Partition)
				msg := &message.Message{}
				proto.Unmarshal(data.Value, msg)
				fmt.Println(msg)
			case <-kh.StopChan:
				return
			}
		}
	})

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	<-sc
	close(kh.StopChan)
	kh.Wg.Wait()
	kh.Clean()
}
