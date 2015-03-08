package main

import (
	// "encoding/binary"
	"flag"
	"fmt"
	// "github.com/Shopify/sarama"
	"github.com/bbangert/toml"
	"github.com/dongzerun/sqltrack/kafka"
	// "io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	// "time"
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

type GlobalConfig struct {
	KafkaConfig *kafka.KafkaInputConfig
}

var cfgs map[string]toml.Primitive

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

}

func loadConfig(configPath *string) *GlobalConfig {
	f, err := os.Open(*configPath)
	if err != nil {
		log.Fatalln("open config file or dir failed: ", *configPath)
	}
	defer f.Close()

	fs, err := os.Stat(*configPath)
	if err != nil {
		log.Fatalln("get stat of file or dir failed: ", *configPath)
	}

	if fs.IsDir() {
		log.Fatalln("config file must be file: ", *configPath)
	}

	if !strings.HasSuffix(*configPath, ".toml") {
		log.Fatalln("config file must has .toml suffix")
	}

	data, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalln("ioutil.ReadAll config file failed: ", *configPath)
	}

	globals := &GlobalConfig{}
	_, err = toml.Decode(string(data), &cfgs)
	if err != nil {
		log.Fatalln("toml.Decode data failed: ", err)
	}
	// log.Println(cfgs)

	// kafkacfg := &KafkaInputConfig{}
	kafkacfg := kafka.NewKafkaInputConfig()

	empty_ignore := map[string]interface{}{}
	parsed_config, ok := cfgs["kafka"]
	if ok {
		if err = toml.PrimitiveDecodeStrict(parsed_config, kafkacfg, empty_ignore); err != nil {
			err = fmt.Errorf("Can't unmarshal config: %s", err)
		}
		// log.Println(kafkacfg)
		globals.KafkaConfig = kafkacfg
	}

	return globals
}
