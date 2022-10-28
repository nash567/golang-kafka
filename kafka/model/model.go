package model

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
)

// Status represents the current status of the topic connection.
type Status int

const (
	StatusStopped Status = iota
	StatusRunning
)

//go:generate enumer -type=Status -json -trimprefix=Status -transform=snake -output=enum_status_gen.go

// TopicListener is the function type for consumer group listener functions.
type TopicListener func(ctx context.Context, msg ConsumerMessage) error

// ProducerMessage is a direct mapping of sarama.ProducerMessage, remove extra fields as required.
type ProducerMessage struct {
	Key       []byte
	Value     []byte
	Headers   []RecordHeader
	Metadata  interface{}
	Partition int32
}

func (p *ProducerMessage) ToSaramaMsg(topic string) *sarama.ProducerMessage {
	headers := make([]sarama.RecordHeader, 0, len(p.Headers))
	for _, v := range p.Headers {
		headers = append(headers, sarama.RecordHeader{
			Key:   v.Key,
			Value: v.Value,
		})
	}
	return &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.ByteEncoder(p.Key),
		Value:     sarama.ByteEncoder(p.Value),
		Headers:   headers,
		Metadata:  p.Metadata,
		Partition: p.Partition,
	}
}

// ConsumerMessage is a direct mapping of sarama.ConsumerMessage, remove extra fields as required.
type ConsumerMessage struct {
	Key, Value     []byte
	Topic          string
	Partition      int32
	Offset         int64
	Timestamp      time.Time       // only set if kafka is version 0.10+, inner message timestamp
	BlockTimestamp time.Time       // only set if kafka is version 0.10+, outer (compressed) block timestamp
	Headers        []*RecordHeader // only set if kafka is version 0.11+
}

type RecordHeader struct {
	Key   []byte
	Value []byte
}
