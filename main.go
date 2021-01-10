package main

import (
	"log"
	"os"
	"os/signal"

	kafka "github.com/Shopify/sarama"
)

func main() {
	produceKafkaMessage()
}

func produceKafkaMessage() {
	producer, err := kafka.NewAsyncProducer([]string{":9094"}, nil)
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

	var enqueued, producerErrors int
ProducerLoop:
	for {
		select {
		case producer.Input() <- &kafka.ProducerMessage{
			Topic: "messages",
			Key:   nil,
			Value: kafka.StringEncoder("testing 123"),
		}:
			enqueued++
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
			producerErrors++
		case <-signals:
			break ProducerLoop
		}
	}

	log.Printf("Enqueued: %d; errors: %d\n", enqueued, producerErrors)
}
