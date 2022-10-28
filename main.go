package main

import (
	"context"
	"encoding/json"
	"example/kafka/kafka"
	"example/kafka/kafka/model"
	"fmt"
	"time"
)

const topic = "suspend.tp-live-topologies"

func listen(ctx context.Context, msg model.ConsumerMessage) error {
	fmt.Println("value recieved from ch", string(msg.Value))
	print(msg)
	return nil
}

func print(v interface{}) {
	bb, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(bb))
}

func main() {

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Minute)
	client, err := kafka.NewClient(ctx, &kafka.Config{
		Brokers: "kafka:9092",
	})
	if err != nil {
		fmt.Printf("Error creating Client  %v", err)

	}
	Topic := client.Connect(ctx, topic)

	consumerGroup, err := Topic.ConsumerGroup(ctx)
	if err != nil {
		fmt.Printf("Error creating consumer group %v", err)

	}
	err = consumerGroup.RegisterListener(ctx, topic, listen)
	if err != nil {
		fmt.Printf("Error Registering consumer   %v", err)

	}
	err = consumerGroup.Start(ctx)
	if err != nil {
		fmt.Printf("Error Starting consumer  %v", err)

	}
	fmt.Println("\nready to listen, produce now\n")
	// time.Sleep(10 * time.Second)
	<-ctx.Done()
}
