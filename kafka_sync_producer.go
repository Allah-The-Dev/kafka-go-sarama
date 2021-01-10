package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/Shopify/sarama"
	kafka "github.com/Shopify/sarama"
)

func produceSyncKafkaMessage() {
	// create producer
	producer, err := initProducer()
	if err != nil {
		fmt.Println("Error producer: ", err.Error())
		os.Exit(1)
	}

	// read command line input
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter msg: \n")
		msg, _ := reader.ReadString('\n')

		// publish without goroutene
		publish(msg, producer)

		// publish with go routene
		// go publish(msg, producer)
	}
}

func initProducer() (kafka.SyncProducer, error) {
	// setup sarama log to stdout
	kafka.Logger = log.New(os.Stdout, "", log.Ltime)

	// producer config
	config := kafka.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = kafka.WaitForAll
	config.Producer.Return.Successes = true

	// async producer
	//prd, err := sarama.NewAsyncProducer([]string{kafkaConn}, config)

	// sync producer
	prd, err := kafka.NewSyncProducer([]string{kafkaConn}, config)

	return prd, err
}

func publish(message string, producer sarama.SyncProducer) {
	// publish sync
	msg := &kafka.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	p, o, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Error publish: ", err.Error())
	}

	// publish async
	//producer.Input() <- &sarama.ProducerMessage{

	fmt.Println("Partition: ", p)
	fmt.Println("Offset: ", o)
}
