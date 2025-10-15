package main

import (
  "fmt"
  "github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
  c, err := kafka.NewConsumer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "group.id":          "my-group",
    "auto.offset.reset": "earliest",
  })
  if err != nil {
    fmt.Printf("Failed to create consumer: %s\n", err)
    return
  }
  defer c.Close()

  err = c.SubscribeTopics([]string{"my-topic"}, nil)
  if err != nil {
    fmt.Printf("Failed to subscribe: %s\n", err)
    return
  }

  for {
    msg, err := c.ReadMessage(-1)
    if err == nil {
      fmt.Printf("Received message: %s\n", string(msg.Value))
    } else {
      fmt.Printf("Consumer error: %v (%T)\n", err, err)
    }
  }
}