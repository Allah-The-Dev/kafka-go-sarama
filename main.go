package main

func main() {
	go produceKafkaMessage()
	consumeKafkaMessage()
}
