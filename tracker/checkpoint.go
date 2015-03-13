package tracker

import (
	"encoding/binary"
	"log"
	"os"
)

func (kh *KafkaHelper) writeCheckpoint() (err error) {
	if kh.checkpointFile == nil {
		if kh.checkpointFile, err = os.OpenFile(kh.kconfig.CheckpointFilename,
			os.O_WRONLY|os.O_SYNC|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
			return
		}
	}

	kh.checkpointFile.Seek(0, 0)
	var i int32
	for i = 0; i < kh.kconfig.PartitionNums; i++ {
		err = binary.Write(kh.checkpointFile, binary.LittleEndian, &kh.checkpointData[i])
	}
	return
}

func (kh *KafkaHelper) setCheckpoint(i int32, offset int64) {
	kh.checkpointData[i] = offset
}

func (kh *KafkaHelper) readCheckpoint(filename string) (err error) {
	file, err := os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()

	var i int32
	for i = 0; i < kh.kconfig.PartitionNums; i++ {
		err = binary.Read(file, binary.LittleEndian, &kh.checkpointData[i])
		log.Println("read checkpoint file err: ", err)
	}
	return
}
