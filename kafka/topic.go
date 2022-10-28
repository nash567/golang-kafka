package kafka

import (
	"context"
	"fmt"

	"example/kafka/kafka/model"
)

// TopicConnector is the actual connection to topic, we should have only one TopicConnector instance per topic.
type TopicConnector struct {
	name          string
	client        *Client
	consumerGroup model.ConsumerGroup // consumer group instance
	producer      model.Producer      // producer instance
}

func (t *TopicConnector) SetName(name string) {
	t.name = name
}

func (t *TopicConnector) SetClient(client *Client) {
	t.client = client
}

func (t *TopicConnector) SetConsumerGroup(consumerGroup model.ConsumerGroup) {
	t.consumerGroup = consumerGroup
}

func (t *TopicConnector) SetProducer(producer model.Producer) {
	t.producer = producer
}

func (t *TopicConnector) GetName() string {
	return t.name
}

// nolint: ireturn // implements kafkaModel.Topic interface
func (t *TopicConnector) Producer(ctx context.Context) (model.Producer, error) {
	if t.producer == nil {
		syncProducer, err := newTopicProducer(ctx, t.name, t.client)
		if err != nil {
			return nil, err
		}
		t.producer = syncProducer
	}
	return t.producer, nil
}

// nolint: ireturn // implements kafkaModel.Topic interface
func (t *TopicConnector) ConsumerGroup(ctx context.Context) (model.ConsumerGroup, error) {
	if t.consumerGroup == nil {
		grp, err := newTopicConsumerGroup(ctx, t.name, t.client)
		if err != nil {
			return nil, err
		}
		t.consumerGroup = grp
	}
	return t.consumerGroup, nil
}

func (t *TopicConnector) Start(ctx context.Context) error {
	if t.consumerGroup != nil && t.consumerGroup.Status() != model.StatusRunning {
		if err := t.consumerGroup.Start(ctx); err != nil {
			return fmt.Errorf("failed to start consumer group: %w", err)
		}
	}
	return nil
}

func (t *TopicConnector) Stop(ctx context.Context) error {
	if t.consumerGroup != nil && t.consumerGroup.Status() != model.StatusStopped {
		if err := t.consumerGroup.Stop(ctx); err != nil {
			return fmt.Errorf("failed to stop consumer group: %w", err)
		}
	}
	return nil
}
