package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"example/kafka/kafka/model"

	"github.com/Shopify/sarama"
)

// we need to have same group id per topic across all the oe instances of the application
// similarly we should have same group id across all the instances of api application
// we can do something like <topic_name>_oe & <topic_name>_api.
// this is to ensure all the consumers across all the instances of same application belong to same consumer group.
func getGroupID(topic string) string {
	return "suspend." + topic
}

type topicConsumerGroup struct {
	groupID   string
	client    sarama.Client
	name      string
	status    model.Status
	listeners map[string]model.TopicListener
	ready     chan struct{}
	grp       sarama.ConsumerGroup
	mux       *sync.RWMutex
}

// Start will start a consumer go routine and start consuming messages in an infinite loop
// from the topic. Also start an error handling go routine which consumer errors from kafka error channel
// for errors to be sent on this channel, set errors config to true in sarama on init.
func (tc *topicConsumerGroup) Start(ctx context.Context) error {
	if tc.Status() == model.StatusRunning {
		return fmt.Errorf("already instatus error")
	}
	go tc.consume(ctx)
	go tc.catchErrors(ctx) // to log kafka errors, ensure "sarama.Config.Consumer.Return.Errors" is set to true
	select {
	case <-ctx.Done():
		return fmt.Errorf("failed to start consumer group: %w", ctx.Err())
	case <-tc.ready:
		tc.mux.Lock()
		close(tc.ready)
		tc.ready = nil
		tc.mux.Unlock()
	}
	if tc.status != model.StatusRunning {
		return fmt.Errorf("failed to start consumer group")
	}
	return nil
}

// Stop closes out the consumer group and sets the status as stopped.
func (tc *topicConsumerGroup) Stop(context.Context) error {
	if tc.Status() == model.StatusStopped {
		return fmt.Errorf("aready in status error")
	}

	if err := tc.grp.Close(); err != nil {
		return fmt.Errorf("failed to stop consumer group: %w", err)
	}
	tc.mux.Lock()
	defer tc.mux.Unlock()
	tc.status = model.StatusStopped
	return nil
}

// Status returns the current status of the consumer group.
func (tc *topicConsumerGroup) Status() model.Status {
	tc.mux.RLock()
	defer tc.mux.RUnlock()
	return tc.status
}

func (tc *topicConsumerGroup) consume(ctx context.Context) {
	timeoutDuration := 10 * time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:

			err := tc.grp.Consume(ctx, []string{tc.name}, tc)
			if err != nil {
				// Log an error and sleep for a timeout, or until the context ends.
				// The timeout is important to avoid spinning if there is a connection problem.
				fmt.Println("failed to consume message from topic")
				select {
				case <-ctx.Done():
					break // context is closed
				case <-time.After(timeoutDuration):
				}
				continue
			}
		}
	}
}

func (tc *topicConsumerGroup) catchErrors(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-tc.grp.Errors():
			fmt.Println("internal/kafka/consumer_group:catchErrors()", err)

		}
	}
}

// RegisterListener registers a listener function with the consumer group.
func (tc *topicConsumerGroup) RegisterListener(_ context.Context, identifier string, listener model.TopicListener) error {
	tc.mux.Lock()
	defer tc.mux.Unlock()
	if tc.listeners == nil {
		tc.listeners = make(map[string]model.TopicListener)
	}
	if _, ok := tc.listeners[identifier]; ok {
		return fmt.Errorf("duplicate id error")
	}
	tc.listeners[identifier] = listener
	return nil
}

func (tc *topicConsumerGroup) UnRegisterListener(_ context.Context, identifier string) error {
	tc.mux.Lock()
	defer tc.mux.Unlock()
	if tc.listeners == nil {
		return fmt.Errorf("no listeners : %v", tc.name)
	}
	if _, ok := tc.listeners[identifier]; !ok {
		return fmt.Errorf("no registered listener :%v", tc.name)
	}
	delete(tc.listeners, identifier)
	return nil
}

// NewTopicConsumerGroup this needs to be called at the start of application for each topic we want to consume
// the consumer group instance can then be injected in engines/services as dependencies.
// We need to call start and stop for each consumerGroup at the start and stop of the application respectively.
func newTopicConsumerGroup(_ context.Context, topic string, client sarama.Client) (*topicConsumerGroup, error) {
	grpID := getGroupID(topic)
	grp, err := sarama.NewConsumerGroupFromClient(grpID, client)
	if err != nil {
		return nil, fmt.Errorf("failed to create new consumer group: %w", err)
	} else {
		fmt.Printf("Sucessfully created group")
	}
	return &topicConsumerGroup{
		status:  model.StatusStopped,
		client:  client,
		groupID: grpID,
		name:    topic,
		grp:     grp,
		ready:   make(chan struct{}),
		mux:     new(sync.RWMutex),
	}, nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (tc *topicConsumerGroup) Setup(_ sarama.ConsumerGroupSession) error {
	tc.mux.Lock()
	defer tc.mux.Unlock()
	tc.status = model.StatusRunning
	if tc.ready != nil {
		tc.ready <- struct{}{}
	}
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (tc *topicConsumerGroup) Cleanup(sarama.ConsumerGroupSession) error {
	tc.mux.Lock()
	defer tc.mux.Unlock()
	tc.status = model.StatusStopped
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (tc *topicConsumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	defer func() {
		if r := recover(); r != nil {

			fmt.Println("recovered from a panic while consuming a topic")
		}
	}()
	tc.mux.RLock()
	defer tc.mux.RUnlock()
	// NOTE: Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		msg := fromSaramaConsumerMessage(message)
		for identifier, fn := range tc.listeners {
			err := fn(context.Background(), msg)
			if err != nil {
				fmt.Println("kafka topic listener failed while consuming a message", identifier)
				continue
			}
		}
		session.MarkMessage(message, "")
	}

	return nil
}

func fromSaramaConsumerMessage(msg *sarama.ConsumerMessage) model.ConsumerMessage {
	headers := make([]*model.RecordHeader, 0, len(msg.Headers))
	for _, v := range msg.Headers {
		headers = append(headers, &model.RecordHeader{
			Key:   v.Key,
			Value: v.Value,
		})
	}
	return model.ConsumerMessage{
		Key:            msg.Key,
		Value:          msg.Value,
		Topic:          msg.Topic,
		Partition:      msg.Partition,
		Offset:         msg.Offset,
		Timestamp:      msg.Timestamp,
		BlockTimestamp: msg.BlockTimestamp,
		Headers:        headers,
	}
}
