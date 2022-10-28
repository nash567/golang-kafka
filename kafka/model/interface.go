package model

import (
	"context"
)

// Topic is the main connection to the topic
// Producer/ConsumerGroup connection can only be fetched from this connection instead of direct Producer instance.
type Topic interface {
	GetName() string
	Producer(ctx context.Context) (Producer, error)
	ConsumerGroup(ctx context.Context) (ConsumerGroup, error)
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

//go:generate mockery --name=Topic --outpkg mocks

// ConsumerGroup is a wrapper over sarama ConsumerGroup.
type ConsumerGroup interface {
	// Start function is to be called at the application startup, this will set status of topicConn to started
	Start(ctx context.Context) error
	// Stop function is called at application shut down time to gracefully close the producer connection
	Stop(ctx context.Context) error
	// Status will return current status of topic conn, currently it is started and stopped
	Status() Status
	// RegisterListener is to be called by the engine/service that wants to listen to messages from a given topic
	RegisterListener(ctx context.Context, identifier string, listener TopicListener) error
	// UnRegisterListener is to be called by the engine/service when it no longer wants to consume messages from this topic or shutting down
	UnRegisterListener(ctx context.Context, identifier string) error
}

//go:generate mockery --name=ConsumerGroup --outpkg mocks

// Producer interface is a wrapper over sarama SyncProducer, all the message production should be done through this interface.
type Producer interface {
	// Produce function is to be used to send message to any kafka topic, it will first check the topic status, before sending any message
	Produce(ctx context.Context, msg *ProducerMessage) (partition int32, offset int64, err error)
}

//go:generate mockery --name=Producer --outpkg mocks

type TopicConnector interface {
	// Connect Creates a new topic connections
	Connect(ctx context.Context, name string) Topic
}

//go:generate mockery --name=TopicConnector --outpkg mocks
