package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"

	kafkaModel "example/kafka/kafka/model"
)

// Client is a wrapper over sarama Client.
type Client struct {
	sarama.Client

	topicConnMap map[string]*TopicConnector
}
type Config struct {
	Brokers string
}

func NewClient(_ context.Context, cfg *Config) (*Client, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V2_1_0_0
	saramaCfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	saramaCfg.Consumer.Fetch.Default = 10 * 1000 * 1000 // 10MB
	saramaCfg.Consumer.Fetch.Max = 10 * 1000 * 1000     // 10MB
	// to use in SyncProducer, this must be set to true
	// if not set, you will get an error like,
	// "Producer.Return.Successes must be true to be used in a SyncProduce"
	saramaCfg.Producer.Return.Successes = true
	sClient, err := sarama.NewClient(strings.Split(cfg.Brokers, ","), saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("error creating saram client: %w", err)
	}
	client := &Client{sClient, make(map[string]*TopicConnector)}

	return client, nil
}

// Connect returns a connection to topic using the Client.
// nolint: ireturn // implements kafkaModel.TopicConnector interface
func (c *Client) Connect(ctx context.Context, name string) kafkaModel.Topic {
	if c.topicConnMap == nil {
		c.topicConnMap = make(map[string]*TopicConnector)
	}
	if conn, ok := c.topicConnMap[name]; ok {
		return conn
	}
	conn := &TopicConnector{
		client:        c,
		name:          name,
		consumerGroup: nil,
		producer:      nil,
	}
	c.topicConnMap[name] = conn
	return conn
}

func (c *Client) Verify() error {
	if len(c.Client.Brokers()) == 0 {
		return fmt.Errorf("no brockers were found")
	}
	for _, broker := range c.Client.Brokers() {
		connected, err := broker.Connected()
		if err != nil {
			return fmt.Errorf("kafka broker connection failed: %w", err)
		}
		if !connected {
			return fmt.Errorf("no brockers were found %v", broker.Addr())
		}
	}
	return nil
}
