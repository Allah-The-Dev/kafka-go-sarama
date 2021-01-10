package main

var (
	kafkaConn         = "172.18.104.177:9094"
	topic             = "messages"
	consumerGroupName = "my-group-1"
)

func main() {
	//go produceAsyncKafkaMessage()
	go produceSyncKafkaMessage()
	consumeKafkaMessage()
}
