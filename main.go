package main

import (
	"flag"
	"fmt"
	"golang.org/x/sync/errgroup"
	"os"
	"strings"
	"log"
	"github.com/Shopify/sarama"
)

var (
	brokers   = flag.String("brokers", "localhost:9092", "The Kafka brokers to connect to, as a comma separated list")
)

func main() {
	printerConsumer := func(m string) error {
		fmt.Println(m)
		return nil
	}

	printerProducer := func(m string) error {
		fmt.Println(m)
		return nil
	}

	check(printerConsumer, printerProducer)

}


func check(printerConsumer Notifier, printerProducer Notifier) {
	producerStateChan := make(chan string)
	consumerStateChan := make(chan string)
	flag.Parse()
	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	config := sarama.NewConfig()

	client, _ :=sarama.NewClient(brokerList, config)

	go checkProducerState(client,"test", producerStateChan)
	go checkConsumerState(client, "test", "my-consumer-group", consumerStateChan)

	//TODO go checkConsumerState
	//TODO get topics, groupIds configurable
	//TODO tests


	for{
		select {
		case msg, _ := <-producerStateChan: notify(msg, printerProducer)
		case msg, _ := <-consumerStateChan: notify(msg, printerConsumer)
		}
	}
}

type Notifier func (string) error

func notify(message string, applies ...Notifier ) error{
		var g errgroup.Group
		for _,apply := range applies {
			g.Go(func() error {
				return apply(message)
			})
		}
		if err := g.Wait(); err != nil{
			return err
		}
		return nil
}

