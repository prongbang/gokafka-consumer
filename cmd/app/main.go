package main

import (
	"fmt"
	"os"

	"github.com/IBM/sarama"
)

func main() {
	// Kafka consumer configuration :9092
	server := os.Getenv("KAFKA_SERVER")
	fmt.Println("KAFKA_SERVER:", server)

	// Create a new Kafka consumer config
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create a new consumer instance
	consumer, err := sarama.NewConsumer([]string{server}, config)
	if err != nil {
		fmt.Println("Failed to create consumer:", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			fmt.Println("Failed to close consumer:", err)
		}
	}()

	// Create a new consumer partition consumer
	partitionConsumer, err := consumer.ConsumePartition("my-topic", 0, sarama.OffsetOldest)
	if err != nil {
		fmt.Println("Failed to start consumer for partition:", err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			fmt.Println("Failed to close consumer for partition:", err)
		}
	}()

	// Consume messages from the partition
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("Received message on topic %s: %s\n", msg.Topic, string(msg.Value))
		case err := <-partitionConsumer.Errors():
			fmt.Printf("Consumer error: %s", err)
		}
	}
}
