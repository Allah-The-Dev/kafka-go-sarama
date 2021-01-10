package main

import (
	"context"
	"fmt"

	kafka "github.com/Shopify/sarama"
)

type exampleConsumerGroupHandler struct{}

func (exampleConsumerGroupHandler) Setup(_ kafka.ConsumerGroupSession) error   { return nil }
func (exampleConsumerGroupHandler) Cleanup(_ kafka.ConsumerGroupSession) error { return nil }
func (h exampleConsumerGroupHandler) ConsumeClaim(sess kafka.ConsumerGroupSession, claim kafka.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message topic:%q partition:%d offset:%d message:%s\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		sess.MarkMessage(msg, "")
	}
	return nil
}

func consumeKafkaMessage() {
	config := kafka.NewConfig()
	config.Version = kafka.V2_6_0_0 // specify appropriate version
	config.Consumer.Return.Errors = true

	group, err := kafka.NewConsumerGroup([]string{kafkaConn}, consumerGroupName, config)
	if err != nil {
		panic(err)
	}
	defer func() { _ = group.Close() }()

	// Track errors
	go func() {
		for err := range group.Errors() {
			fmt.Println("ERROR", err)
		}
	}()

	// Iterate over consumer sessions.
	ctx := context.Background()
	for {
		topics := []string{topic}
		handler := exampleConsumerGroupHandler{}

		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		err := group.Consume(ctx, topics, handler)
		if err != nil {
			panic(err)
		}
	}
}
