package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	topic := "HVSE"
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "foo_data",
		"auto.offset.reset": "smallest",
	})
	if err != nil {
		fmt.Printf("Failed to create consumer: %s\n", err)
	}
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		fmt.Printf("Failed to subscribe: %s\n", err)
	}

	for {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("data team reading order: %s\n", string(e.Value))
		case *kafka.Error:
			fmt.Printf("Error: %v: %v\n", e.Code(), e)
		}
	}
}
