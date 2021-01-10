package main

import (
	"log"
	"os"
	"os/signal"

	kafka "github.com/Shopify/sarama"
)

func produceAsyncKafkaMessage() {
	config := kafka.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := kafka.NewAsyncProducer([]string{kafkaConn}, nil)

	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var enqueued, producerSuccesses, producerErrors int

	for {
		select {
		case producer.Input() <- &kafka.ProducerMessage{
			Topic: topic,
			Key:   nil,
			Value: kafka.StringEncoder("testing 123"),
		}:
			enqueued++
		case success := <-producer.Successes():
			log.Printf(" -- -- Message topic:%q partition:%d offset:%d message length:%d\n", success.Topic, success.Partition, success.Offset, success.Value.Length())
			producerSuccesses++
			producer.AsyncClose()
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
			producerErrors++
		case <-signals:
			break
		}
	}

}
