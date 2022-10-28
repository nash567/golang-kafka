package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/Shopify/sarama"
)

const (
	brokers = "kafka:9092"
	topic   = "suspend.tp-live-topologies"
)

func main() {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_1_0_0
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.MaxMessageBytes = 1024 * 1000 * 1000 // 10MB

	client, err := sarama.NewClient(strings.Split(brokers, ","), cfg)
	if err != nil {
		log.Fatal("could not create kafka client:", err)
		return
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Fatal("could not create producer from client:", err)
		return
	}
	defer func() {
		if err = producer.Close(); err != nil {
			log.Fatal("could not close producer:", err)
		}
	}()
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder([]byte("hey i am sending a message to you,..........................")),
	}
	if _, _, err := producer.SendMessage(msg); err != nil {
		fmt.Printf("failed to send topology message: %v", err)

	} else {
		fmt.Printf("succesfully message send")
	}
}
