package rocketmq

import (
	"context"
	"errors"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	mqp "github.com/apache/rocketmq-client-go/v2/producer"
)

/**
 *  https://help.aliyun.com/document_detail/114479.html
 */
type NativeRocketMQProducer struct {
	md     *Metadata
	client rocketmq.Producer
}

func init() {
	Producers[TCPNativeProto] = &NativeRocketMQProducer{}
}

// NewRocketMQProducer
func (mq *NativeRocketMQProducer) Init(md *Metadata) error {
	var opts []mqp.Option
	mq.md = md
	if len(md.NameServer) > 0 {
		opts = append(opts, mqp.WithNameServer(splitAndTrim(md.NameServer)))
	} else {
		return errors.New("get nameserver failed")
	}

	if len(md.NameServerDomain) > 0 {
		opts = append(opts, mqp.WithNameServerDomain(md.NameServerDomain))
	}

	if md.Retries > 0 {
		opts = append(opts, mqp.WithRetry(md.Retries))
	}

	if len(md.AccessKey) > 0 {
		var credentials = primitive.Credentials{
			AccessKey: md.AccessKey,
			SecretKey: md.SecretKey,
		}
		opts = append(opts, mqp.WithCredentials(credentials))
	}

	opts = append(opts, mqp.WithNamespace(md.InstanceId))
	var err error
	mq.client, err = rocketmq.NewProducer(opts...)
	if err != nil {
		return err
	}
	return mq.Start()
}

// Start the Producer
func (mq *NativeRocketMQProducer) Start() error {
	return mq.client.Start()
}

// Shutdown the Producer
func (mq *NativeRocketMQProducer) Shutdown() error {
	return mq.client.Shutdown()
}

// Send sync message, support single msg just now
func (mq *NativeRocketMQProducer) SendSync(ctx context.Context, msgs ...*primitive.Message) (*primitive.SendResult, error) {
	for _, m := range msgs {
		return mq.client.SendSync(context.Background(), m)
	}
	return nil, nil
}

// Send async message, unimplemented
func (mq *NativeRocketMQProducer) SendAsync(ctx context.Context, f func(ctx context.Context, result *primitive.SendResult, err error), msg ...*primitive.Message) error {
	return errors.New("unimplemented method")
}

// Send oneway message, unimplemented
func (mq *NativeRocketMQProducer) SendOneWay(ctx context.Context, msg ...*primitive.Message) error {
	return errors.New("unimplemented method")
}
