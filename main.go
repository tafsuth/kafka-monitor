package main

import (
	"flag"
	"os"
	"time"
	"fmt"
	"strings"
	"log"
	"github.com/Shopify/sarama"
	"golang.org/x/sync/errgroup"
)


var (
	brokers   = flag.String("brokers", "localhost:9092", "The Kafka brokers to connect to, as a comma separated list")
)

func main() {
	messages := make(chan string)
	go checkState(messages)

	notify(messages,
		func(m string) error {
		fmt.Println(m)
		return nil
	}, func(m string) error {
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

func checkState( messages chan string) {
	flag.Parse()
	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	config := sarama.NewConfig()

	client, _ :=sarama.NewClient(brokerList, config)

	ticker := time.NewTicker(5 * time.Second)

	lastOffset := int64(0)
	for range ticker.C {
		requestedLastOffset, err := GetCurrentMaxTopicOffset(client,"test")
		if err != nil {
			panic(err)
		}
		if lastOffset == requestedLastOffset {
			messages <- fmt.Sprintln("still same offset", lastOffset)
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
