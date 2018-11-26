package main

import (
	"strings"
	"log"
	"github.com/Shopify/sarama"
)


func main() {
	produceMessageInKafka()  //go produceMessageInKafka ne fait rien, check why
}

func produceMessageInKafka() {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(strings.Split("localhost:9092", ",") , config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer: ", err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Println("Failed to close server", err)
		}
	}()

	producer.SendMessage(&sarama.ProducerMessage{
		Topic: "test",
		Value: sarama.StringEncoder("blablablabla"),
	})
}