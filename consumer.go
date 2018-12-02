package main

import (
	"github.com/bsm/sarama-cluster"
	"fmt"
	"os"
	"log"
	"github.com/Shopify/sarama"
)


func consumeOneMessage() {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	brokers := []string{"localhost:9092"}
	topics := []string{"test"}
	consumer, err := cluster.NewConsumer(brokers, "my-consumer-group", topics, config)

	if err != nil {
		panic(err)
	}
	defer consumer.Close()
	// consume errors
	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	for msg := range consumer.Messages() {
		fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
		consumer.MarkOffset(msg, "")	// mark message as processed
		return
	}
}