package kafka

import (
	"context"
	"fmt"

	"example/kafka/kafka/model"

	"github.com/Shopify/sarama"
)

type topicProducer struct {
	name         string
	syncProducer sarama.SyncProducer
}

// SendMessage synchronously produce message to kafka topic if in running state.
func (tp *topicProducer) Produce(ctx context.Context, msg *model.ProducerMessage) (partition int32, offset int64, err error) {
	select {
	case <-ctx.Done():
		return 0, 0, fmt.Errorf("message not sent(topic:%s): %w", tp.name, ctx.Err())
	default:
		partition, offset, err := tp.syncProducer.SendMessage(msg.ToSaramaMsg(tp.name))
		if err != nil {
			return 0, 0, fmt.Errorf("error sending message(topic:%v):%w", tp.name, err)
		}
		return partition, offset, nil
	}
}

func newTopicProducer(ctx context.Context, name string, client sarama.Client) (*topicProducer, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("producer not initialized(topic:%s): %w", name, ctx.Err())
	default:
		sp, err := sarama.NewSyncProducerFromClient(client)
		if err != nil {
			return nil, fmt.Errorf("error creating sync producer for topic(%s):%w", name, err)
		}
		return &topicProducer{
			name:         name,
			syncProducer: sp,
		}, nil
	}
}
