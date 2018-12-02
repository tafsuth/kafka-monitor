package main

import (
	"time"
	"fmt"
	"github.com/Shopify/sarama"
)

func checkProducerState(client sarama.Client, topic string, messages chan string) {

	ticker := time.NewTicker(5 * time.Second)

	lastOffset := int64(0)
	for range ticker.C {
		requestedLastOffset, err := GetCurrentMaxTopicOffset(client,topic)
		if err != nil {
			panic(err)
		}
		if lastOffset == requestedLastOffset {
			messages <- fmt.Sprint("producer - still same offset: ", lastOffset)
		}

		lastOffset = requestedLastOffset
	}
}

func GetCurrentMaxTopicOffset(client sarama.Client, topic string) (int64, error) {
	partitions, err := client.Partitions(topic)
	if err != nil {
		return 0, err
	}

	offsets := make([]int64, len(partitions))

	for _, partition := range partitions {
		offsets[partition], err = client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			return 0, err
		}
	}

	offsetMax := int64(0)
	for _, e := range offsets {
		if e > offsetMax {
			offsetMax = e
		}
	}

	return offsetMax, nil

}
