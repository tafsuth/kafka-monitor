package main

import (
	"flag"
	"os"
	"strings"
	"log"
	"github.com/Shopify/sarama"
)


var (
	brokers   = flag.String("brokers", os.Getenv("KAFKA_ADVERTISED_HOST_NAME"), "The Kafka brokers to connect to, as a comma separated list")
)

func main() {
	flag.Parse()
	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer: ", err, brokerList)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Println("Failed to close server", err)
		}
	}()

	producer.SendMessage(&sarama.ProducerMessage{
		Topic: "important",
		Value: sarama.StringEncoder("ceci est mon message: RAA"),
	})
}