package main

import (
	"flag"
	"fmt"
	"github.com/dongzerun/sqltrack/kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	configPath = flag.String("config", "/tmp/kafka.toml", "config must be file")
)

// [kafkaInput]
// topic = "mysql.slow.ms"
// addrs = ["g1-db-srv-00:9092", "g1-db-srv-01:9092", "g1-db-srv-02:9092"]
// encoder = "ProtobufEncoder"
// partitioner = "Random"
// offsetmethod = "Manual"

func main() {

	flag.Parse()
	globals := loadConfig(configPath)
	log.Println(globals.KafkaConfig)
	log.Println(globals.KafkaConfig.Addrs)
	// ConsumerWithSelect()

	kh := kafka.NewKafkaHelper(globals.KafkaConfig)
	go kh.StartPull()
	for {
		select {
		case data := <-kh.MsgKafka:
			fmt.Println(data.Topic, data.Key, data.Offset, data.Partition)
		}
	}
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
