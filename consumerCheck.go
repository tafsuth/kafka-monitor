package main

import (
	"log"
	"github.com/Shopify/sarama"
	"time"
	"fmt"
)

func checkConsumerState (client sarama.Client, topic string, groupId string, messages chan string) {

	ticker := time.NewTicker(5 * time.Second)

	lastOffset := int64(0)
	for range ticker.C {
		requestedLastOffset, err := getConsumerOffset(client,topic, groupId)
		if err != nil {
			panic(err)
		}
		if lastOffset == requestedLastOffset {
			messages <- fmt.Sprint("consumer - still same offset: ", lastOffset)
		}

		lastOffset = requestedLastOffset
	}
}

func getConsumerOffset (client sarama.Client, topic string, groupId string) (int64, error)  {

	offsetManager, err := sarama.NewOffsetManagerFromClient(groupId, client)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := offsetManager.Close(); err != nil {
			log.Println(err)
		}
	}()

	var offsets []int64

	partitions, err := client.Partitions(topic)
	for _, partition := range partitions {
		partManager, err := offsetManager.ManagePartition(topic, partition)
		if err != nil {
			return 0, err
		}
		defer func() {
			if err := partManager.Close(); err != nil {
				log.Println(err)
			}
		}()

		offset, _ := partManager.NextOffset()
		offsets = append(offsets, offset)
	}

	offsetMax := int64(0)
	for _, e := range offsets {
		if e > offsetMax {
			offsetMax = e
		}
	}

	return offsetMax, nil
}
