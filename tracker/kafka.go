package tracker

import (
	"github.com/Shopify/sarama"
	"github.com/dongzerun/sqltrack/util"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

func init() {
	RegisterIns("kafka", func() InputSource { return &KafkaHelper{} })
}

// &{0 0 <nil> <nil> <nil> <nil>
// [] [] <nil>
// []
// <nil> <nil>
// {{{0 0} 0 0 <nil>}}
// <nil>}
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

	// MsgKafka chan *sarama.ConsumerMessage
	MsgKafka chan InputMsg
	StopChan chan bool
	wg       util.WaitGroupWrapper

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
		// mostcase default 1 partition
		PartitionNums: 1,
		OffsetMethod:  "Newest",
	}
}

func (kh *KafkaHelper) InitHelper(g *GlobalConfig) {
	if g.KafkaConfig == nil {
		log.Fatalln("kafka config must be set!!!")
	}

	kh.kconfig = g.KafkaConfig
	kh.MsgKafka = make(chan InputMsg, 30)
	kh.StopChan = make(chan bool, 1)

	// kh := &KafkaHelper{
	// 	kconfig:  g.KafkaConfig,
	// 	MsgKafka: make(chan InputMsg, 30),
	// 	StopChan: make(chan bool, 1),
	// }
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

	go kh.StartPull()
	kh.wg.Wrap(kh.persistOffset)
}

func (kh *KafkaHelper) StartPull() {
	log.Println("Start Pull data")
	var i int32

	for i = 0; i < kh.kconfig.PartitionNums; i++ {
		kh.wg.Add(1)
		go func(i int32) {
			for {
				select {
				case err := <-kh.consumerP[i].Errors():
					log.Fatalln(err)
				case data := <-kh.consumerP[i].Messages():
					kh.MsgKafka <- KafkaMsgWrap{m: data}
					kh.setCheckpoint(i, data.Offset)
				case <-kh.StopChan:
					goto exit
				}
			}
		exit:
			kh.wg.Done()
		}(i)
	}
}

// input/kafka.go:155: cannot use <-kh.MsgKafka (type InputMsg) as
// type *KafkaMsgWrap in return argument: need type assertion
func (kh *KafkaHelper) Consume() <-chan InputMsg {
	return kh.MsgKafka
}

// 持久化kafka offset数据 当前每秒写入文件
// zookeeper是个选择？
func (kh *KafkaHelper) persistOffset() {
	tk := time.NewTicker(time.Second * 1)
	for {
		select {
		case <-tk.C:
			kh.writeCheckpoint()
		case <-kh.StopChan:
			goto exit
		}
	}
exit:
	tk.Stop()
}

func (kh *KafkaHelper) Stop() bool {
	return true
}

func (kh *KafkaHelper) Clean() {
	close(kh.StopChan)
	var i int32
	for i = 0; i < kh.kconfig.PartitionNums; i++ {
		kh.consumerP[i].Close()
	}
	kh.client.Close()
	kh.checkpointFile.Sync()
	kh.checkpointFile.Close()
	kh.wg.Wait()
}
