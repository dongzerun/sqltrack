package tracker

import (
	"github.com/Shopify/sarama"
)

type KafkaMsgWrap struct {
	m *sarama.ConsumerMessage
}

func (k KafkaMsgWrap) GetTopic() string {
	return k.m.Topic
}
func (k KafkaMsgWrap) GetOffset() int64 {
	return k.m.Offset
}
func (k KafkaMsgWrap) GetValue() []byte {
	return k.m.Value
}
func (k KafkaMsgWrap) GetKey() []byte {
	return k.m.Key
}
func (k KafkaMsgWrap) GetPartition() int32 {
	return k.m.Partition
}
