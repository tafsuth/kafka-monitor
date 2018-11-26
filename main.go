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
	messages := make(chan string)
	flag.Parse()
	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	config := sarama.NewConfig()

	client, _ :=sarama.NewClient(brokerList, config)

	go checkProducerState(client, messages, "test")

	//TODO go checkConsumerState
	//TODO get topics, groupIds configurable
	//TODO tests

	notify(messages,
		func(m string) error {
			//TODO send email
			//TODO send slack notification
			fmt.Println(m)
			return nil
		})
}

type Notifier func (string) error

func notify(messages chan string, applies ... Notifier ) error{
	for m := range messages {
		var g errgroup.Group
		for _,apply := range applies {
			g.Go(func() error {
				return apply(m)
			})
		}
		if err := g.Wait(); err != nil{
			return err
		}

	}
	return nil
}

