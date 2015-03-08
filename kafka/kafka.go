package kafka

import (
	// "encoding/binary"
	// "flag"
	// "fmt"
	"github.com/Shopify/sarama"
	// "github.com/bbangert/toml"
	"io"
	// "io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

type KafkaHelper struct {
	// 处理计数
	processMessageCount    int64
	processMessageFailures int64

	//kafka相关结构体
	clientConfig   *sarama.ClientConfig
	consumerConfig *sarama.ConsumerConfig
	client         *sarama.Client
	consumer       *sarama.Consumer

	// PartitionConsumer PartitionConsumerConfig 数组一一对应
	consumerP       []*sarama.PartitionConsumer
	partitionConfig []*sarama.PartitionConsumerConfig

	//Offset 当前没有保存到zk中，保存在文件里
	checkpointFile *os.File
	//数组和KafkaInputConfig.PartitionNums 一致
	checkpointData []int64

	MsgKafka chan *sarama.ConsumerMessage
	stopChan chan bool

	//配置
	kconfig *KafkaInputConfig
}

type KafkaInputConfig struct {
	Addrs string `toml:"addrs"`
	// Consumer Config
	Topic string `toml:"topic"`
	// 多个partition并发读取 default 1
	PartitionNums      int32  `toml:"partition_nums"`
	Group              string `toml:"group"`
	OffsetMethod       string `toml:"offset_method"` // Manual, Newest, Oldest
	CheckpointFilename string `toml:"checkpoint_filename"`
}

func NewKafkaInputConfig() *KafkaInputConfig {
	return &KafkaInputConfig{
		PartitionNums: 3,
		OffsetMethod:  "Newest",
	}
}

func NewKafkaHelper(cfg *KafkaInputConfig) *KafkaHelper {
	kh := &KafkaHelper{
		kconfig:  cfg,
		MsgKafka: make(chan *sarama.ConsumerMessage, 1024),
	}
	var err error
	kh.client, err = sarama.NewClient("myclient", strings.Split(kh.kconfig.Addrs, ","), kh.clientConfig)
	if err != nil {
		log.Fatalln("new kafka client failed: ", err)
	} else {
		log.Println("kh.client connected :", kh.kconfig.Addrs)
	}

	kh.consumer, err = sarama.NewConsumer(kh.client, kh.consumerConfig)
	if err != nil {
		log.Fatalln("new consumer failed: ", err)
	} else {
		log.Println("new consumer connected")
	}

	// 	checkpointData:  make([]int64, kh.kconfig.PartitionNums),
	// consumerP:       make(*sarama.PartitionConsumer, kh.kconfig.PartitionNums),
	// partitionConfig: make(*sarama.PartitionConsumerConfig, kh.kconfig.PartitionNums),
	kh.checkpointData = make([]int64, kh.kconfig.PartitionNums)
	kh.consumerP = make([]*sarama.PartitionConsumer, kh.kconfig.PartitionNums)
	kh.partitionConfig = make([]*sarama.PartitionConsumerConfig, kh.kconfig.PartitionNums)

	var i int32
	switch kh.kconfig.OffsetMethod {
	case "Manual":
		if err = kh.readCheckpoint(kh.kconfig.CheckpointFilename); err != nil && err != io.EOF {
			log.Fatalln("warning read checkpoint failed", err)
		}
		for i = 0; i < kh.kconfig.PartitionNums; i++ {
			cfg := sarama.NewPartitionConsumerConfig()
			cfg.OffsetMethod = sarama.OffsetMethodManual
			log.Println("in Manual method set partition: ", i, " offset: ", kh.checkpointData[i])
			cfg.OffsetValue = kh.checkpointData[i]
			kh.partitionConfig[i] = cfg
		}
	case "Newest":
		for i = 0; i < kh.kconfig.PartitionNums; i++ {
			cfg := sarama.NewPartitionConsumerConfig()
			cfg.OffsetMethod = sarama.OffsetMethodNewest
			kh.partitionConfig[i] = cfg
		}
	case "Oldest":
		for i = 0; i < kh.kconfig.PartitionNums; i++ {
			cfg := sarama.NewPartitionConsumerConfig()
			cfg.OffsetMethod = sarama.OffsetMethodOldest
			kh.partitionConfig[i] = cfg
		}
	default:
		log.Fatalln("unknow kafka OffsetMethod")
	}

	for i = 0; i < kh.kconfig.PartitionNums; i++ {
		p, err := kh.consumer.ConsumePartition(kh.kconfig.Topic, i, kh.partitionConfig[i])
		if err != nil {
			log.Fatalln("create ConsumePartition failed on: ", i, "msg: ", err)
		}
		kh.consumerP[i] = p
	}

	return kh
}

func (kh *KafkaHelper) StartPull() {
	log.Println("Start Pull data")
	var i int32
	for i = 0; i < kh.kconfig.PartitionNums; i++ {
		go func(i int32) {
			for {
				select {
				case err := <-kh.consumerP[i].Errors():
					log.Fatalln(err)
				case data := <-kh.consumerP[i].Messages():
					kh.MsgKafka <- data
					kh.setCheckpoint(i, data.Offset)
				case <-kh.stopChan:
					return
				}
			}
		}(i)
	}

	// 如果是手动指定offset,那么每1秒去持久化数据
	if kh.kconfig.OffsetMethod == "Manual" {
		go func() {
			tk := time.NewTicker(time.Second * 1)
			select {
			case <-tk.C:
				kh.writeCheckpoint()
			case <-kh.stopChan:
				return
			}
		}()
	}

	// 通过close(kh.stopChan)来关闭所有goroutine
	<-kh.stopChan
}
