package main

import (

	"testing"
)

func Test_Main(t *testing.T) {
	consumerStatus := make(chan string, 1)
	producerStatus := make(chan string, 1)

	produceOneMessage()
	consumeOneMessage()
	getConsumerStatus := func(m string) error {
		defer close(consumerStatus)
		consumerStatus <- m
		return nil
	}


	getProducerStatus := func(m string) error {
		defer close(producerStatus)
		producerStatus <- m
		return nil
	}


	go check(getConsumerStatus, getProducerStatus)

	m := <- consumerStatus
	if m != "consumer - still same offset: 1" {
		t.Error("one message should be consumed", m)
	}


	m = <- producerStatus
	if m != "producer - still same offset: 1" {
		t.Error("one message should be consumed", m)
	}



}



