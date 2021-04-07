package rocketmq

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	mqc "github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

const (
	DefaultHttpAccessProto = "http"
	TCPCGOProto            = "tcp-cgo"
	TCPNativeProto         = "tcp"

	defaultConsumerNumOfMessages      = 1
	defaultHttpMQClientTimeoutSeconds = 30
	defaultHttpConsumerWaitSeconds    = 0
	defaultHttpConsumerTimeoutSeconds = 60
	defaultRocketMQChannel            = "INNER"
	defaultRocketMQPublicChannel      = "AlibabaCloud/DefaultChannel"
)

var (
	Producers = make(map[string]Producer)
	Consumers = make(map[string]PushConsumer)
)

type PushConsumer interface {
	Init(md *Metadata) error

	// Start the PullConsumer for consuming message
	Start() error

	// Shutdown the PullConsumer, all offset of MessageQueue will be sync to broker before process exit
	Shutdown() error

	// Subscribe a topic for consuming
	Subscribe(topic string, selector consumer.MessageSelector, f func(context.Context, ...*primitive.MessageExt) (mqc.ConsumeResult, error)) error

	// Unsubscribe a topic
	Unsubscribe(topic string) error
}

type Producer interface {
	Init(md *Metadata) error
	Start() error
	Shutdown() error
	SendSync(ctx context.Context, mq ...*primitive.Message) (*primitive.SendResult, error)
	SendAsync(ctx context.Context, mq func(ctx context.Context, result *primitive.SendResult, err error),
		msg ...*primitive.Message) error
	SendOneWay(ctx context.Context, mq ...*primitive.Message) error
}
