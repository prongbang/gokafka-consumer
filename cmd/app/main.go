package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
)

func main() {
	// Kafka consumer configuration
	server := os.Getenv("KAFKA_SERVER")
	fmt.Println("KAFKA_SERVER:", server)

	// Create a new Kafka consumer configuration
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create a new Kafka consumer instance
	consumer, err := sarama.NewConsumer([]string{server}, config)
	if err != nil {
		fmt.Printf("Failed to create Kafka consumer: %s\n", err)
		os.Exit(1)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			fmt.Printf("Failed to close Kafka consumer: %s\n", err)
		}
	}()

	// Create a new Kafka consumer group handler
	consumerGroup, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "my-group", config)
	if err != nil {
		fmt.Printf("Failed to create Kafka consumer group: %s\n", err)
		os.Exit(1)
	}
	defer consumerGroup.Close()

	// Start consuming messages
	go func() {
		for {
			err := consumerGroup.Consume(context.Background(), nil, &consumers{})
			if err != nil {
				fmt.Printf("Error during message consumption: %s\n", err)
			}
		}
	}()

	// Wait for interrupt signal
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	<-sigchan
}

// consumer represents a Sarama consumer group consumer
type consumers struct{}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *consumers) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *consumers) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages()
func (consumer *consumers) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		fmt.Printf("Message claimed: value = %s, timestamp = %v, topic = %s\n", string(message.Value), message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
	}

	return nil
}
